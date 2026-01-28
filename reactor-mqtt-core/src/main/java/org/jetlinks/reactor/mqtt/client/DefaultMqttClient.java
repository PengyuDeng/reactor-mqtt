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
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 基于 Reactor Netty 的 MQTT 客户端 - 纯响应式、高性能
 *
 * @author PengyuDeng
 */
class DefaultMqttClient implements MqttClient {

    private final MqttClientConfig config;

    DefaultMqttClient() {
        config = new MqttClientConfig();
    }

    @Override
    public MqttClient host(String host) {
        config.setHost(host);
        return this;
    }

    @Override
    public MqttClient port(int port) {
        config.setPort(port);
        return this;
    }

    @Override
    public MqttClient clientId(String clientId) {
        config.setClientId(clientId);
        return this;
    }

    @Override
    public MqttClient handlePublishing(Function<ClientReceivedPublish, Mono<Void>> handler) {
        config.setPublishingHandler(handler);
        return this;
    }

    @Override
    public MqttClient auth(String username, String password) {
        config.setUsername(username);
        config.setPassword(password != null ? password.getBytes() : null);
        return this;
    }

    @Override
    public MqttClient auth(String username, byte[] password) {
        config.setUsername(username);
        config.setPassword(password);
        return this;
    }

    @Override
    public MqttClient keepAlive(int seconds) {
        config.setKeepAlive(seconds);
        return this;
    }

    @Override
    public MqttClient cleanSession(boolean cleanSession) {
        config.setCleanSession(cleanSession);
        return this;
    }

    @Override
    public MqttClient protocolVersion(MqttVersion version) {
        config.setProtocolVersion(version);
        return this;
    }

    @Override
    public MqttClient maxMessageSize(int maxMessageSize) {
        config.setMaxMessageSize(maxMessageSize);
        return this;
    }

    @Override
    public MqttClient will(String topic, ByteBuf payload, MqttQoS qos, boolean retain) {
        config.setWillMessage(new MqttWillMessage(topic, payload, qos, retain));
        return this;
    }

    @Override
    public MqttClient will(String topic, byte[] payload, MqttQoS qos, boolean retain) {
        return will(topic, payload != null ? Unpooled.wrappedBuffer(payload) : null, qos, retain);
    }

    @Override
    public MqttClient will(MqttWillMessage willMessage) {
        config.setWillMessage(willMessage);
        return this;
    }

    @Override
    public MqttClient ssl(SslContext sslContext) {
        config.setSslContext(sslContext);
        return this;
    }

    @Override
    public MqttClient reconnectStrategy(ReconnectStrategy strategy) {
        config.setReconnectStrategy(strategy != null ? strategy : ReconnectStrategy.none());
        return this;
    }

    @Override
    public MqttClient autoResubscribe(boolean autoResubscribe) {
        config.setAutoResubscribe(autoResubscribe);
        return this;
    }

    @Override
    public MqttClient autoAck(boolean autoAck) {
        config.setAutoAck(autoAck);
        return this;
    }

    @Override
    public MqttClient qos(MqttQoS qos) {
        config.setQos(qos);
        return this;
    }

    @Override
    public MqttClient loopResources(LoopResources loopResources) {
        config.setLoopResources(loopResources);
        return this;
    }

    @Override
    public MqttClient tcpNoDelay(boolean tcpNoDelay) {
        config.setTcpNoDelay(tcpNoDelay);
        return this;
    }

    @Override
    public MqttClient connectTimeout(Duration timeout) {
        config.setConnectTimeout(timeout);
        return this;
    }

    @Override
    public MqttClient subscribeTimeout(Duration timeout) {
        config.setSubscribeTimeout(timeout);
        return this;
    }

    @Override
    public MqttClient unsubscribeTimeout(Duration timeout) {
        config.setUnsubscribeTimeout(timeout);
        return this;
    }

    @Override
    public MqttClient publishTimeout(Duration timeout) {
        config.setPublishTimeout(timeout);
        return this;
    }

    @Override
    public MqttClient subscriptionManager(SubscriptionManager subscriptionManager) {
        config.setSubscriptionManager(subscriptionManager);
        return this;
    }

    @Override
    public Mono<ClientConnection> connect() {
        config.getClientId();
        final Supplier<TcpClient> tcpClientSupplier = this::createTcpClient;

        return createTcpClient()
                .connect()
                .map(conn -> new DefaultClientConnection(conn, config, tcpClientSupplier))
                .flatMap(DefaultClientConnection::initialize);
    }

    @Override
    public ClientConnection connectNow() {
        return connect()
                .timeout(config.getConnectTimeout())
                .block();
    }

    private TcpClient createTcpClient() {
        TcpClient tcpClient = TcpClient.create()
                                       .host(config.getHost())
                                       .port(config.getPort())
                                       .option(ChannelOption.TCP_NODELAY, config.isTcpNoDelay())
                                       .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) config
                                               .getConnectTimeout()
                                               .toMillis())
                                       .doOnConnected(conn -> {
                                           conn.addHandlerFirst("mqttEncoder", MqttEncoder.INSTANCE);
                                           conn.addHandlerFirst("mqttDecoder", new MqttDecoder(config.getMaxMessageSize()));
                                       });

        if (config.getLoopResources() != null) {
            tcpClient = tcpClient.runOn(config.getLoopResources());
        }

        if (config.getSslContext() != null) {
            tcpClient = tcpClient.secure(spec -> spec.sslContext(config.getSslContext()));
        }

        return tcpClient;
    }
}
