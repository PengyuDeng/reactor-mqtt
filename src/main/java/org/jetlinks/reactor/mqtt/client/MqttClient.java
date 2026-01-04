/*
 * Copyright 2025
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
package org.jetlinks.reactor.mqtt.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.handler.ssl.SslContext;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;

import java.time.Duration;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 基于 Reactor Netty 的 MQTT 客户端 - 纯响应式、高性能
 *
 * <h3>基本用法：</h3>
 * <pre>{@code
 * MqttClient.create()
 *     .host("127.0.0.1")
 *     .port(1883)
 *     .clientId("my-client")
 *     .handlePublishing(pub -> {
 *         System.out.println("Received: " + pub.getTopic());
 *         return Mono.empty();
 *     })
 *     .connect()
 *     .flatMap(conn -> {
 *         // 订阅
 *         conn.subscribe("/topic", msg -> Mono.empty());
 *         // 发布
 *         return conn.publish("/topic", payload, MqttQoS.AT_LEAST_ONCE)
 *                    .then(conn.onClose());
 *     })
 *     .subscribe();
 * }</pre>
 *
 * <h3>自动重连：</h3>
 * <pre>{@code
 * MqttClient.create()
 *     .reconnectStrategy(ReconnectStrategy.exponentialBackoff(
 *         Duration.ofSeconds(1),
 *         Duration.ofMinutes(5)))
 *     .connect()
 *     .subscribe();
 * }</pre>
 *
 * @author PengyuDeng
 */
public class MqttClient {

    /**
     * MQTT 服务器主机地址,默认 127.0.0.1
     */
    private String host = "127.0.0.1";

    /**
     * MQTT 服务器端口,默认 1883
     */
    private int port = 1883;

    /**
     * MQTT 客户端 ID,为 null 时自动生成
     */
    private String clientId;

    /**
     * MQTT 连接用户名,可选
     */
    private String username;

    /**
     * MQTT 连接密码,可选
     */
    private byte[] password;

    /**
     * MQTT Keep Alive 时间(秒),默认 60 秒
     */
    private short keepAliveSeconds = 60;

    /**
     * 是否使用 Clean Session,默认 true
     * 服务端不保留任何会话状态，连接断开后，所有订阅信息被清除，离线期间的 QoS 1/2 消息不会被保存，每次连接都是全新的会话。
     */
    private boolean cleanSession = true;

    /**
     * MQTT 协议版本,默认 MQTT 3.1.1
     */
    private byte protocolVersion = MqttVersion.MQTT_3_1_1.protocolLevel();

    /**
     * MQTT 消息最大大小(字节),默认 8096
     */
    private int maxMessageSize = 8096;

    /**
     * 遗言消息
     */
    private WillMessage willMessage;

    /**
     * SSL 上下文,用于加密连接,为 null 时使用明文连接
     */
    private SslContext sslContext;

    /**
     * 重连策略,默认不重连
     */
    private ReconnectStrategy reconnectStrategy = ReconnectStrategy.none();

    /**
     * 重连后是否自动重新订阅之前的主题,默认 true
     */
    private boolean autoResubscribe = true;

    /**
     * 全局消息发布处理器,接收所有订阅的消息
     */
    private Function<MqttClientPublishing, Mono<Void>> publishingHandler;

    /**
     * 是否自动确认 QoS 1/2 消息,默认 true
     */
    private boolean autoAck = true;

    /**
     * 默认 QoS 级别,用于 publish 和 subscribe 方法未指定 QoS 时,默认 QoS 0
     */
    private MqttQoS qos = MqttQoS.AT_MOST_ONCE;

    /**
     * Netty EventLoop 资源,为 null 时使用默认
     */
    private LoopResources loopResources;

    /**
     * 是否启用 TCP_NODELAY(禁用 Nagle 算法),默认 true
     */
    private boolean tcpNoDelay = true;

    /**
     * TCP 连接超时时间,默认 10 秒
     */
    private Duration connectTimeout = Duration.ofSeconds(10);

    private MqttClient() {
    }

    public static MqttClient create() {
        return new MqttClient();
    }

    public MqttClient host(String host) {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host must not be null or blank");
        }
        this.host = host;
        return this;
    }

    public MqttClient port(int port) {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port must be between 0 and 65535");
        }
        this.port = port;
        return this;
    }

    public MqttClient clientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    public MqttClient auth(String username, String password) {
        this.username = username;
        this.password = password != null ? password.getBytes() : null;
        return this;
    }

    public MqttClient auth(String username, byte[] password) {
        this.username = username;
        this.password = password;
        return this;
    }

    public MqttClient keepAlive(short seconds) {
        if (seconds < 0) {
            throw new IllegalArgumentException("keepAlive must not be negative");
        }
        this.keepAliveSeconds = seconds;
        return this;
    }

    public MqttClient cleanSession(boolean cleanSession) {
        this.cleanSession = cleanSession;
        return this;
    }

    /**
     * 设置 MQTT 协议版本
     *
     * @param version 协议版本
     */
    public MqttClient protocolVersion(MqttVersion version) {
        if (version == null) {
            throw new IllegalArgumentException("protocolVersion must not be null");
        }
        this.protocolVersion = version.protocolLevel();
        return this;
    }

    public MqttClient maxMessageSize(int maxMessageSize) {
        if (maxMessageSize <= 0) {
            throw new IllegalArgumentException("maxMessageSize must be positive");
        }
        this.maxMessageSize = maxMessageSize;
        return this;
    }

    /**
     * 设置遗言消息
     */
    public MqttClient will(String topic, ByteBuf payload, MqttQoS qos, boolean retain) {
        this.willMessage = new WillMessage(topic, payload, qos, retain);
        return this;
    }

    public MqttClient will(String topic, byte[] payload, MqttQoS qos, boolean retain) {
        return will(topic, payload != null ? Unpooled.wrappedBuffer(payload) : null, qos, retain);
    }

    public MqttClient will(WillMessage willMessage) {
        this.willMessage = willMessage;
        return this;
    }

    public MqttClient ssl(SslContext sslContext) {
        this.sslContext = sslContext;
        return this;
    }

    /**
     * 设置重连策略
     */
    public MqttClient reconnectStrategy(ReconnectStrategy strategy) {
        this.reconnectStrategy = strategy != null ? strategy : ReconnectStrategy.none();
        return this;
    }

    /**
     * 启用/禁用重连
     */
    public MqttClient reconnect(boolean enable) {
        if (enable) {
            this.reconnectStrategy = ReconnectStrategy.exponentialBackoff(
                    Duration.ofSeconds(1), Duration.ofMinutes(5));
        } else {
            this.reconnectStrategy = ReconnectStrategy.none();
        }
        return this;
    }

    /**
     * 重连后是否自动恢复订阅
     */
    public MqttClient autoResubscribe(boolean autoResubscribe) {
        this.autoResubscribe = autoResubscribe;
        return this;
    }

    /**
     * 设置全局消息处理器
     */
    public MqttClient handlePublishing(Function<MqttClientPublishing, Mono<Void>> handler) {
        this.publishingHandler = handler;
        return this;
    }

    /**
     * 设置是否自动确认 QoS 1/2 消息
     */
    public MqttClient autoAck(boolean autoAck) {
        this.autoAck = autoAck;
        return this;
    }

    /**
     * 设置默认 QoS 级别
     * <p>
     * 用于 publish 和 subscribe 方法未指定 QoS 时的默认值。
     * </p>
     *
     * @param qos 默认 QoS 级别
     */
    public MqttClient qos(MqttQoS qos) {
        if (qos == null) {
            throw new IllegalArgumentException("defaultQos must not be null");
        }
        this.qos = qos;
        return this;
    }

    public MqttClient loopResources(LoopResources loopResources) {
        this.loopResources = loopResources;
        return this;
    }

    public MqttClient tcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
        return this;
    }

    public MqttClient connectTimeout(Duration timeout) {
        this.connectTimeout = timeout;
        return this;
    }

    /**
     * 异步连接
     *
     * @return 连接完成后返回 MqttClientConnection
     */
    public Mono<MqttClientConnection> connect() {
        String actualClientId = clientId != null ? clientId :
                "reactor-mqtt-" + UUID.randomUUID().toString().substring(0, 8);

        Sinks.One<MqttClientConnection> connectionSink = Sinks.one();

        TcpClient tcpClient = TcpClient.create()
                                       .host(host)
                                       .port(port)
                                       .option(ChannelOption.TCP_NODELAY, tcpNoDelay)
                                       .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) connectTimeout.toMillis())
                                       .doOnConnected(conn -> {
                                           conn.addHandlerFirst("mqttEncoder", MqttEncoder.INSTANCE);
                                           conn.addHandlerFirst("mqttDecoder", new MqttDecoder(maxMessageSize));
                                       });

        if (loopResources != null) {
            tcpClient = tcpClient.runOn(loopResources);
        }

        if (sslContext != null) {
            tcpClient = tcpClient.secure(spec -> spec.sslContext(sslContext));
        }

        final Supplier<TcpClient> tcpClientSupplier = this::createTcpClient;

        return tcpClient.connect()
                        .flatMap(conn -> {
                            DefaultMqttClientConnection mqttConn = new DefaultMqttClientConnection(
                                    conn,
                                    actualClientId,
                                    username,
                                    password,
                                    keepAliveSeconds,
                                    cleanSession,
                                    protocolVersion,
                                    willMessage,
                                    publishingHandler,
                                    autoAck,
                                    qos,
                                    reconnectStrategy,
                                    autoResubscribe,
                                    tcpClientSupplier
                            );
                            return mqttConn.initialize();
                        });
    }

    /**
     * 同步连接
     *
     * @return 连接对象
     * @throws RuntimeException 如果连接超时或失败
     */
    public MqttClientConnection connectNow() {
        return connect()
                .timeout(connectTimeout.plusSeconds(5))
                .block();
    }

    /**
     * 同步连接，带超时
     *
     * @param timeout 超时时间
     * @return 连接对象
     * @throws RuntimeException 如果连接超时或失败
     */
    public MqttClientConnection connectNow(Duration timeout) {
        return connect()
                .timeout(timeout)
                .block();
    }

    private TcpClient createTcpClient() {
        TcpClient client = TcpClient.create()
                                    .host(host)
                                    .port(port)
                                    .option(ChannelOption.TCP_NODELAY, tcpNoDelay)
                                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) connectTimeout.toMillis())
                                    .doOnConnected(conn -> {
                                        conn.addHandlerFirst("mqttEncoder", MqttEncoder.INSTANCE);
                                        conn.addHandlerFirst("mqttDecoder", new MqttDecoder(maxMessageSize));
                                    });

        if (loopResources != null) {
            client = client.runOn(loopResources);
        }

        if (sslContext != null) {
            client = client.secure(spec -> spec.sslContext(sslContext));
        }

        return client;
    }
}
