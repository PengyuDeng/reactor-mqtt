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
import org.jetlinks.reactor.mqtt.MqttWillMessage;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;

import java.time.Duration;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * 基于 Reactor Netty 的 MQTT 客户端 - 纯响应式、高性能
 *
 * @author PengyuDeng
 */
class DefaultMqttClient implements MqttClient {

    private String host = "127.0.0.1";
    private int port = 1883;
    private String clientId;
    private String username;
    private byte[] password;
    private short keepAliveSeconds = 60;
    private boolean cleanSession = true;
    private byte protocolVersion = MqttVersion.MQTT_3_1_1.protocolLevel();
    private int maxMessageSize = 8096;
    private MqttWillMessage willMessage;
    private SslContext sslContext;
    private ReconnectStrategy reconnectStrategy = ReconnectStrategy.none();
    private boolean autoResubscribe = true;
    private java.util.function.Function<ClientReceivedPublish, Mono<Void>> publishingHandler;
    private boolean autoAck = true;
    private MqttQoS qos = MqttQoS.AT_MOST_ONCE;
    private LoopResources loopResources;
    private boolean tcpNoDelay = true;
    private Duration connectTimeout = Duration.ofSeconds(10);
    private Duration subscribeTimeout = Duration.ofSeconds(10);
    private Duration unsubscribeTimeout = Duration.ofSeconds(10);
    private Duration publishTimeout = Duration.ofSeconds(30);
    private SubscriptionManager subscriptionManager;

    DefaultMqttClient() {
    }

    @Override
    public MqttClient host(String host) {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host must not be null or blank");
        }
        this.host = host;
        return this;
    }

    @Override
    public MqttClient port(int port) {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port must be between 0 and 65535");
        }
        this.port = port;
        return this;
    }

    @Override
    public MqttClient clientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    @Override
    public MqttClient handlePublishing(java.util.function.Function<ClientReceivedPublish, Mono<Void>> handler) {
        this.publishingHandler = handler;
        return this;
    }

    @Override
    public MqttClient auth(String username, String password) {
        this.username = username;
        this.password = password != null ? password.getBytes() : null;
        return this;
    }

    @Override
    public MqttClient auth(String username, byte[] password) {
        this.username = username;
        this.password = password;
        return this;
    }

    @Override
    public MqttClient keepAlive(short seconds) {
        if (seconds < 0) {
            throw new IllegalArgumentException("keepAlive must not be negative");
        }
        this.keepAliveSeconds = seconds;
        return this;
    }

    @Override
    public MqttClient cleanSession(boolean cleanSession) {
        this.cleanSession = cleanSession;
        return this;
    }

    @Override
    public MqttClient protocolVersion(MqttVersion version) {
        if (version == null) {
            throw new IllegalArgumentException("protocolVersion must not be null");
        }
        this.protocolVersion = version.protocolLevel();
        return this;
    }

    @Override
    public MqttClient maxMessageSize(int maxMessageSize) {
        if (maxMessageSize <= 0) {
            throw new IllegalArgumentException("maxMessageSize must be positive");
        }
        this.maxMessageSize = maxMessageSize;
        return this;
    }

    @Override
    public MqttClient will(String topic, ByteBuf payload, MqttQoS qos, boolean retain) {
        this.willMessage = new MqttWillMessage(topic, payload, qos, retain);
        return this;
    }

    @Override
    public MqttClient will(String topic, byte[] payload, MqttQoS qos, boolean retain) {
        return will(topic, payload != null ? Unpooled.wrappedBuffer(payload) : null, qos, retain);
    }

    @Override
    public MqttClient will(MqttWillMessage willMessage) {
        this.willMessage = willMessage;
        return this;
    }

    @Override
    public MqttClient ssl(SslContext sslContext) {
        this.sslContext = sslContext;
        return this;
    }

    @Override
    public MqttClient reconnectStrategy(ReconnectStrategy strategy) {
        this.reconnectStrategy = strategy != null ? strategy : ReconnectStrategy.none();
        return this;
    }

    @Override
    public MqttClient reconnect(boolean enable) {
        if (enable) {
            this.reconnectStrategy = ReconnectStrategy.exponentialBackoff(
                    Duration.ofSeconds(1), Duration.ofMinutes(5));
        } else {
            this.reconnectStrategy = ReconnectStrategy.none();
        }
        return this;
    }

    @Override
    public MqttClient autoResubscribe(boolean autoResubscribe) {
        this.autoResubscribe = autoResubscribe;
        return this;
    }

    @Override
    public MqttClient autoAck(boolean autoAck) {
        this.autoAck = autoAck;
        return this;
    }

    @Override
    public MqttClient qos(MqttQoS qos) {
        if (qos == null) {
            throw new IllegalArgumentException("defaultQos must not be null");
        }
        this.qos = qos;
        return this;
    }

    @Override
    public MqttClient loopResources(LoopResources loopResources) {
        this.loopResources = loopResources;
        return this;
    }

    @Override
    public MqttClient tcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
        return this;
    }

    public MqttClient connectTimeout(Duration timeout) {
        this.connectTimeout = timeout;
        return this;
    }

    @Override
    public MqttClient subscribeTimeout(Duration timeout) {
        this.subscribeTimeout = timeout;
        return this;
    }

    @Override
    public MqttClient unsubscribeTimeout(Duration timeout) {
        this.unsubscribeTimeout = timeout;
        return this;
    }

    @Override
    public MqttClient publishTimeout(Duration timeout) {
        this.publishTimeout = timeout;
        return this;
    }

    @Override
    public MqttClient subscriptionManager(SubscriptionManager subscriptionManager) {
        this.subscriptionManager = subscriptionManager;
        return this;
    }

    @Override
    public Mono<ClientConnection> connect() {
        String actualClientId = clientId != null ? clientId :
                "reactor-mqtt-" + UUID.randomUUID().toString().substring(0, 8);

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
                            Consumer<ClientReceivedPublish> consumer = null;
                            if (publishingHandler != null) {
                                consumer = pub -> publishingHandler.apply(pub).subscribe();
                            }
                            DefaultClientConnection mqttConn = new DefaultClientConnection(
                                    conn,
                                    actualClientId,
                                    username,
                                    password,
                                    keepAliveSeconds,
                                    cleanSession,
                                    protocolVersion,
                                    willMessage,
                                    consumer,
                                    autoAck,
                                    qos,
                                    reconnectStrategy,
                                    autoResubscribe,
                                    tcpClientSupplier,
                                    subscribeTimeout,
                                    unsubscribeTimeout,
                                    publishTimeout,
                                    subscriptionManager
                            );
                            return mqttConn.initialize();
                        });
    }

    @Override
    public ClientConnection connectNow() {
        return connect()
                .timeout(connectTimeout)
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
