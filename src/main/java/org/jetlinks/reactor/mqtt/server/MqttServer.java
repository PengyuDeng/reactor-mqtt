/*
 * Copyright 2025 JetLinks https://www.jetlinks.cn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jetlinks.reactor.mqtt.server;

import io.netty.channel.ChannelOption;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleStateHandler;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.DisposableServer;
import reactor.netty.NettyInbound;
import reactor.netty.NettyOutbound;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpServer;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 基于 Reactor Netty 的 MQTT Server - 纯响应式、高性能
 *
 * <h3>性能优化特性：</h3>
 * <ul>
 *   <li><b>EventLoop 线程复用</b> - 使用 LoopResources 避免线程切换开销</li>
 *   <li><b>TCP 快速回收 & 重用</b> - SO_REUSEADDR 启用端口快速重用</li>
 *   <li><b>低延迟</b> - TCP_NODELAY 禁用 Nagle 算法</li>
 *   <li><b>零拷贝</b> - 直接传递 ByteBuf，避免数据复制</li>
 *   <li><b>纯响应式</b> - 所有 IO 操作均为非阻塞</li>
 * </ul>
 *
 * <h3>基本用法：</h3>
 * <pre>{@code
 * DisposableServer server = MqttServer
 *     .create()
 *     .host("0.0.0.0")
 *     .port(1883)
 *     .handle(connection -> {
 *         connection.handleMessage()
 *             .flatMap(msg -> {
 *                 System.out.println("Received: " + msg.getTopic());
 *                 return msg.acknowledge();
 *             })
 *             .subscribe();
 *
 *         return connection.accept();
 *     })
 *     .bindNow();
 *
 * server.onDispose().block();
 * }</pre>
 *
 * <h3>高性能配置示例：</h3>
 * <pre>{@code
 * // 复用 EventLoop 线程池
 * LoopResources loops = LoopResources.create("mqtt-shared-", 4, true);
 *
 * DisposableServer server = MqttServer
 *     .create()
 *     .port(1883)
 *     .loopResources(loops)           // 复用线程池
 *     .workerCount(4)                 // 工作线程数
 *     .tcpNoDelay(true)               // 禁用 Nagle 算法
 *     .soBacklog(2048)                // 增大连接队列
 *     .writeBufferWaterMark(64*1024, 128*1024) // 调整写缓冲区
 *     .handle(connection -> {
 *         // 业务逻辑必须是非阻塞的！
 *         // 数据库/缓存操作必须使用 reactive client
 *         return connection.accept();
 *     })
 *     .bindNow();
 * }</pre>
 *
 * <h3>TLS 配置（可选）：</h3>
 * <pre>{@code
 * SslContext sslContext = SslContextBuilder.forServer(certFile, keyFile).build();
 * MqttServer.create()
 *     .ssl(sslContext)
 *     .bindNow();
 * }</pre>
 *
 * <p><b>注意事项：</b></p>
 * <ul>
 *   <li>所有业务逻辑必须是非阻塞的</li>
 *   <li>数据库/缓存操作必须使用 reactive client（如 R2DBC, Lettuce）</li>
 *   <li>避免在 EventLoop 线程中执行阻塞操作</li>
 *   <li>无 TLS 场景下性能最优</li>
 * </ul>
 *
 * @author PengyuDeng
 */
public class MqttServer {

    private static final Logger log = Logger.getLogger(MqttServer.class.getName());

    private String host = "127.0.0.1";
    private int port = 1883;
    private int maxMessageSize = 8096;
    private Duration idleTimeout = Duration.ofSeconds(120);
    private SslContext sslContext;
    private Function<ServerConnection, Mono<Void>> connectionHandler;

    private LoopResources loopResources;
    private int workerCount = Runtime.getRuntime().availableProcessors();
    private boolean tcpNoDelay = true;
    private boolean tcpKeepAlive = true;
    private int soBacklog = 1024;
    private int writeBufferLow = 32 * 1024;
    private int writeBufferHigh = 64 * 1024;

    private MqttServer() {
    }

    public static MqttServer create() {
        return new MqttServer();
    }

    public MqttServer host(String host) {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host must not be null or blank");
        }
        this.host = host;
        return this;
    }

    public MqttServer port(int port) {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 0 and 65535, got: " + port);
        }
        this.port = port;
        return this;
    }

    public MqttServer maxMessageSize(int maxMessageSize) {
        if (maxMessageSize <= 0) {
            throw new IllegalArgumentException("maxMessageSize must be positive, got: " + maxMessageSize);
        }
        this.maxMessageSize = maxMessageSize;
        return this;
    }

    public MqttServer idleTimeout(Duration idleTimeout) {
        if (idleTimeout != null && idleTimeout.isNegative()) {
            throw new IllegalArgumentException("idleTimeout must not be negative, got: " + idleTimeout);
        }
        this.idleTimeout = idleTimeout;
        return this;
    }

    public MqttServer ssl(SslContext sslContext) {
        this.sslContext = sslContext;
        return this;
    }

    /**
     * 设置自定义 LoopResources（EventLoop 线程池）
     * 复用线程池可避免线程切换开销
     */
    public MqttServer loopResources(LoopResources loopResources) {
        this.loopResources = loopResources;
        return this;
    }

    /**
     * 设置工作线程数（默认为 CPU 核心数）
     */
    public MqttServer workerCount(int workerCount) {
        if (workerCount <= 0) {
            throw new IllegalArgumentException("workerCount must be positive, got: " + workerCount);
        }
        this.workerCount = workerCount;
        return this;
    }

    /**
     * 设置 TCP_NODELAY（禁用 Nagle 算法，减少延迟）
     */
    public MqttServer tcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
        return this;
    }

    /**
     * 设置 TCP KeepAlive
     */
    public MqttServer tcpKeepAlive(boolean tcpKeepAlive) {
        this.tcpKeepAlive = tcpKeepAlive;
        return this;
    }

    /**
     * 设置 SO_BACKLOG（连接队列大小）
     */
    public MqttServer soBacklog(int soBacklog) {
        if (soBacklog <= 0) {
            throw new IllegalArgumentException("soBacklog must be positive, got: " + soBacklog);
        }
        this.soBacklog = soBacklog;
        return this;
    }

    /**
     * 设置写缓冲区水位线
     */
    public MqttServer writeBufferWaterMark(int low, int high) {
        if (low <= 0) {
            throw new IllegalArgumentException("writeBufferLow must be positive, got: " + low);
        }
        if (high <= 0) {
            throw new IllegalArgumentException("writeBufferHigh must be positive, got: " + high);
        }
        if (low > high) {
            throw new IllegalArgumentException("writeBufferLow must be <= writeBufferHigh, got low: " + low + ", high: " + high);
        }
        this.writeBufferLow = low;
        this.writeBufferHigh = high;
        return this;
    }

    /**
     * 设置连接处理器
     *
     * @param handler 连接处理函数
     */
    public MqttServer handle(Function<ServerConnection, Mono<Void>> handler) {
        this.connectionHandler = handler;
        return this;
    }

    /**
     * 异步绑定服务器
     */
    public Mono<? extends DisposableServer> bind() {
        return createTcpServer().bind();
    }

    /**
     * 同步绑定服务器
     */
    public DisposableServer bindNow() {
        return createTcpServer().bindNow();
    }

    /**
     * 同步绑定服务器，带超时
     */
    public DisposableServer bindNow(Duration timeout) {
        return createTcpServer().bindNow(timeout);
    }

    private TcpServer createTcpServer() {
        TcpServer server = TcpServer.create()
                                    .host(host)
                                    .port(port)
                                    .runOn(getLoopResources())
                                    .option(ChannelOption.SO_REUSEADDR, true)
                                    .option(ChannelOption.SO_BACKLOG, soBacklog)
                                    .childOption(ChannelOption.TCP_NODELAY, tcpNoDelay)
                                    .childOption(ChannelOption.SO_KEEPALIVE, tcpKeepAlive)
                                    .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(writeBufferLow, writeBufferHigh))
                                    .doOnConnection(this::initPipeline)
                                    .handle(this::handle);

        return sslContext != null
                ? server.secure(spec -> spec.sslContext(sslContext))
                : server;
    }


    private LoopResources getLoopResources() {
        return loopResources != null
                ? loopResources
                : LoopResources.create("mqtt-", workerCount, true);
    }

    private void initPipeline(Connection connection) {
        connection.addHandlerFirst("mqttEncoder", MqttEncoder.INSTANCE);
        connection.addHandlerFirst("mqttDecoder", new MqttDecoder(maxMessageSize));

        if (idleTimeout != null && !idleTimeout.isZero()) {
            connection.addHandlerFirst("idleStateHandler", new IdleStateHandler(0, 0, idleTimeout.toSeconds(), TimeUnit.SECONDS));
        }
    }

    private Publisher<Void> handle(NettyInbound inbound, NettyOutbound outbound) {
        return new DefaultServerConnection(inbound, outbound).run(this::invokeHandler);
    }


    private Mono<Void> invokeHandler(ServerConnection serverConnection) {
        if (connectionHandler == null) {
            return serverConnection.accept();
        }
        return connectionHandler.apply(serverConnection)
                                .onErrorResume(err -> {
                                    log.log(Level.SEVERE, "处理 MQTT 连接时出错: " + err.getMessage(), err);
                                    return serverConnection.reject(MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE);
                                });
    }
}
