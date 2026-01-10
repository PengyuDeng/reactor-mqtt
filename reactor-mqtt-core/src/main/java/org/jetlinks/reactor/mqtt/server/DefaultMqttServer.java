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
 * @author PengyuDeng
 */
public class DefaultMqttServer implements MqttServer {

    private static final Logger log = Logger.getLogger(DefaultMqttServer.class.getName());

    private String host = "127.0.0.1";
    private int port = 1883;
    private int maxMessageSize = 8096;
    private Duration idleTimeout = Duration.ofSeconds(120);
    private SslContext sslContext;
    private Function<ServerConnection, Mono<Void>> connectionHandler;
    private ServerConnectionListener connectionListener = ServerConnectionListener.empty();
    private MqttAuthenticator authenticator = MqttAuthenticator.allowAnonymous();

    private LoopResources loopResources;
    private int workerCount = Runtime.getRuntime().availableProcessors();
    private boolean tcpNoDelay = true;
    private boolean tcpKeepAlive = true;
    private int soBacklog = 1024;
    private int writeBufferLow = 32 * 1024;
    private int writeBufferHigh = 64 * 1024;

    /**
     * 创建一个新的 DefaultMqttServer 实例
     */
    public DefaultMqttServer() {
    }

    @Override
    public MqttServer host(String host) {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host must not be null or blank");
        }
        this.host = host;
        return this;
    }

    @Override
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

    public MqttServer loopResources(LoopResources loopResources) {
        this.loopResources = loopResources;
        return this;
    }

    public MqttServer workerCount(int workerCount) {
        if (workerCount <= 0) {
            throw new IllegalArgumentException("workerCount must be positive, got: " + workerCount);
        }
        this.workerCount = workerCount;
        return this;
    }

    public MqttServer tcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
        return this;
    }

    public MqttServer tcpKeepAlive(boolean tcpKeepAlive) {
        this.tcpKeepAlive = tcpKeepAlive;
        return this;
    }

    public MqttServer soBacklog(int soBacklog) {
        if (soBacklog <= 0) {
            throw new IllegalArgumentException("soBacklog must be positive, got: " + soBacklog);
        }
        this.soBacklog = soBacklog;
        return this;
    }

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

    @Override
    public MqttServer handle(Function<ServerConnection, Mono<Void>> handler) {
        this.connectionHandler = handler;
        return this;
    }

    /**
     * 设置连接事件监听器
     *
     * @param listener 事件监听器
     * @return MqttServer
     */
    public MqttServer connectionListener(ServerConnectionListener listener) {
        this.connectionListener = listener != null ? listener : ServerConnectionListener.empty();
        return this;
    }

    @Override
    public MqttServer authenticator(MqttAuthenticator authenticator) {
        this.authenticator = authenticator != null ? authenticator : MqttAuthenticator.allowAnonymous();
        return this;
    }

    @Override
    public Mono<? extends DisposableServer> bind() {
        return createTcpServer().bind();
    }

    @Override
    public DisposableServer bindNow() {
        return createTcpServer().bindNow();
    }

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
        return new DefaultServerConnection(inbound, outbound, connectionListener).run(this::invokeHandler);
    }


    private Mono<Void> invokeHandler(ServerConnection serverConnection) {
        // 先进行认证
        return authenticator.authenticate(serverConnection)
                            .flatMap(authenticated -> {
                                if (!authenticated) {
                                    log.log(Level.WARNING, "Client " + serverConnection.getClientId() + " authentication failed");
                                    return serverConnection.reject(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD);
                                }

                                // 认证通过，执行用户的 handler
                                if (connectionHandler == null) {
                                    return serverConnection.accept();
                                }
                                return connectionHandler.apply(serverConnection)
                                                        .onErrorResume(err -> {
                                                            log.log(Level.SEVERE, "处理 MQTT 连接时出错: " + err.getMessage(), err);
                                                            return serverConnection.reject(MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE);
                                                        });
                            });
    }
}
