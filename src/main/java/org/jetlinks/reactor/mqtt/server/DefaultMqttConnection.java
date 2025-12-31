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
package org.jetlinks.reactor.mqtt.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.*;
import io.netty.util.ReferenceCountUtil;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.Connection;
import reactor.netty.NettyOutbound;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 基于 Reactor Netty 的 MQTT 连接实现 - 纯响应式
 *
 * @author PengyuDeng
 */
public class DefaultMqttConnection implements MqttConnection {

    private static final Logger log = Logger.getLogger(DefaultMqttConnection.class.getName());

    private final Connection connection;
    private final NettyOutbound outbound;

    private volatile String clientId = "unknown";
    private volatile MqttConnectMessage connectMessage;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean accepted = new AtomicBoolean(false);
    private final AtomicLong lastPingTime = new AtomicLong();
    private final AtomicLong keepAliveTimeoutMs = new AtomicLong(120_000L);

    private final Sinks.One<MqttConnectMessage> connectSink = Sinks.one();
    private final Sinks.Empty<Void> disposeSink = Sinks.empty();

    private volatile Consumer<MqttPublishing> publishingHandler;
    private volatile Consumer<MqttSubscription> subscribeHandler;
    private volatile Consumer<MqttUnSubscription> unsubscribeHandler;

    public DefaultMqttConnection(Connection connection) {
        this.connection = connection;
        this.outbound = connection.outbound();
        this.lastPingTime.set(System.currentTimeMillis());

        connection.onDispose(() -> {
            if (closed.compareAndSet(false, true)) {
                disposeSink.tryEmitEmpty();
            }
        });

        startInboundHandling();
    }

    private void startInboundHandling() {
        connection.inbound()
                  .receiveObject()
                  .cast(MqttMessage.class)
                  .subscribe(
                      this::handleMqttMessageSync,
                      err -> log.log(Level.WARNING, "Inbound error", err),
                      () -> {},
                      subscription -> subscription.request(Long.MAX_VALUE)
                  );
    }

    private void handleMqttMessageSync(MqttMessage msg) {
        lastPingTime.set(System.currentTimeMillis());
        MqttMessageType type = msg.fixedHeader().messageType();

        if (type == MqttMessageType.CONNECT) {
            MqttConnectMessage connectMsg = (MqttConnectMessage) msg;
            this.connectMessage = connectMsg;
            this.clientId = connectMsg.payload().clientIdentifier();
            int keepAliveSeconds = connectMsg.variableHeader().keepAliveTimeSeconds();
            this.keepAliveTimeoutMs.set((keepAliveSeconds + 10) * 1000L);
            connectSink.tryEmitValue(connectMsg);
            return;
        }

        if (!accepted.get()) {
            return;
        }

        switch (type) {
            case PUBLISH -> handlePublishSync((MqttPublishMessage) msg);
            case PUBREC -> handlePubRec((MqttMessageIdVariableHeader) msg.variableHeader()).subscribe();
            case PUBREL -> handlePubRel((MqttMessageIdVariableHeader) msg.variableHeader()).subscribe();
            case SUBSCRIBE -> handleSubscribeMsg((MqttSubscribeMessage) msg).subscribe();
            case UNSUBSCRIBE -> handleUnsubscribeMsg((MqttUnsubscribeMessage) msg).subscribe();
            case PINGREQ -> handlePingReq().subscribe();
            case DISCONNECT -> close().subscribe();
            default -> {}
        }
    }

    private void handlePublishSync(MqttPublishMessage msg) {
        if (publishingHandler != null) {
            DefaultMqttPublishing publishing = new DefaultMqttPublishing(msg, clientId, this::send);
            publishingHandler.accept(publishing);

            if (msg.fixedHeader().qosLevel() != MqttQoS.AT_MOST_ONCE) {
                ReferenceCountUtil.retain(msg);
                publishing.acknowledge()
                         .doFinally(signal -> publishing.release())
                         .subscribe();
            }
        }
    }

    private Mono<Void> handleSubscribeMsg(MqttSubscribeMessage msg) {
        DefaultMqttSubscription sub = new DefaultMqttSubscription(msg, this::send);
        if (subscribeHandler != null) {
            subscribeHandler.accept(sub);
        }
        return sub.acknowledge();
    }

    private Mono<Void> handleUnsubscribeMsg(MqttUnsubscribeMessage msg) {
        DefaultMqttUnSubscription unsub = new DefaultMqttUnSubscription(msg, this::send);
        if (unsubscribeHandler != null) {
            unsubscribeHandler.accept(unsub);
        }
        return unsub.acknowledge();
    }

    private Mono<Void> handlePubRec(MqttMessageIdVariableHeader header) {
        MqttMessage pubRel = new MqttMessage(
                new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                MqttMessageIdVariableHeader.from(header.messageId())
        );
        return send(pubRel);
    }

    private Mono<Void> handlePubRel(MqttMessageIdVariableHeader header) {
        MqttMessage pubComp = new MqttMessage(
                new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_MOST_ONCE, false, 0),
                MqttMessageIdVariableHeader.from(header.messageId())
        );
        return send(pubComp);
    }

    private Mono<Void> handlePingReq() {
        MqttMessage pingResp = new MqttMessage(
                new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0)
        );
        return send(pingResp);
    }

    private Mono<Void> send(Object msg) {
        return outbound.sendObject(Mono.just(msg)).then();
    }

    public Mono<MqttConnectMessage> awaitConnect() {
        return connectSink.asMono().timeout(Duration.ofSeconds(10));
    }

    @Override
    public String getClientId() {
        return clientId;
    }

    @Override
    public MqttAuth getAuth() {
        if (connectMessage == null || !connectMessage.variableHeader().hasUserName()) {
            return MqttAuth.empty();
        }
        String username = connectMessage.payload().userName();
        byte[] pwd = connectMessage.payload().passwordInBytes();
        String password = pwd != null ? new String(pwd) : "";
        return new MqttAuth(username, password);
    }

    @Override
    public Mono<Void> reject(MqttConnectReturnCode code) {
        return Mono.defer(() -> {
            if (closed.get()) {
                return Mono.empty();
            }
            MqttConnAckMessage connAck = MqttMessageBuilders.connAck()
                                                            .returnCode(code)
                                                            .sessionPresent(false)
                                                            .build();
            return send(connAck).then(close());
        });
    }

    @Override
    public Mono<Void> accept() {
        return Mono.defer(() -> {
            if (!accepted.compareAndSet(false, true)) {
                return Mono.empty();
            }
            MqttConnAckMessage connAck = MqttMessageBuilders.connAck()
                                                            .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED)
                                                            .sessionPresent(false)
                                                            .build();
            return send(connAck)
                    .doOnSuccess(v -> {
                        if (log.isLoggable(Level.FINE)) {
                            log.fine("MQTT client [" + clientId + "] connected");
                        }
                    });
        });
    }

    @Override
    public MqttWill getWill() {
        if (connectMessage == null || !connectMessage.variableHeader().isWillFlag()) {
            return MqttWill.EMPTY;
        }
        byte[] willPayload = connectMessage.payload().willMessageInBytes();
        String topic = connectMessage.payload().willTopic();
        ByteBuf payload = willPayload != null ? Unpooled.wrappedBuffer(willPayload) : null;
        MqttQoS qos = MqttQoS.valueOf(connectMessage.variableHeader().willQos());
        boolean retain = connectMessage.variableHeader().isWillRetain();
        return new MqttWill(true, topic, payload, qos, retain, MqttProperties.NO_PROPERTIES);
    }

    @Override
    public MqttConnection handlePublishing(Consumer<MqttPublishing> handler) {
        this.publishingHandler = handler;
        return this;
    }

    @Override
    public MqttConnection handleSubscribe(Consumer<MqttSubscription> handler) {
        this.subscribeHandler = handler;
        return this;
    }

    @Override
    public MqttConnection handleUnsubscribe(Consumer<MqttUnSubscription> handler) {
        this.unsubscribeHandler = handler;
        return this;
    }

    @Override
    public Mono<Void> publish(MqttPublishMessage message) {
        return send(message);
    }

    @Override
    public Mono<Void> onDispose() {
        return disposeSink.asMono();
    }

    @Override
    public boolean isAlive() {
        return !closed.get() && connection.channel().isActive();
    }

    @Override
    public Mono<Void> close() {
        return Mono.defer(() -> {
            if (!closed.compareAndSet(false, true)) {
                return Mono.empty();
            }
            return Mono.fromRunnable(() -> {
                disposeSink.tryEmitEmpty();
                connection.dispose();
            });
        });
    }

    @Override
    public long getLastPingTime() {
        return lastPingTime.get();
    }

    @Override
    public Duration getKeepAliveTimeout() {
        return Duration.ofMillis(keepAliveTimeoutMs.get());
    }

    @Override
    public Mono<Void> setKeepAliveTimeout(Duration duration) {
        return Mono.fromRunnable(() -> keepAliveTimeoutMs.set(duration.toMillis()));
    }

    @Override
    public InetSocketAddress getClientAddress() {
        try {
            return (InetSocketAddress) connection.channel().remoteAddress();
        } catch (Exception e) {
            return null;
        }
    }
}
