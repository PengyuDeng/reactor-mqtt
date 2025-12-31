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
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.Connection;
import reactor.netty.NettyOutbound;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.jetlinks.reactor.mqtt.server.MqttConnectionState.*;

/**
 * 基于 Reactor Netty 的 MQTT 连接实现 - 纯响应式
 *
 * @author PengyuDeng
 */
public class DefaultMqttConnection implements MqttConnection {

    private static final Logger log = Logger.getLogger(DefaultMqttConnection.class.getName());

    private static final VarHandle STATE;
    private static final VarHandle LAST_PING_TIME;
    private static final VarHandle KEEP_ALIVE_TIMEOUT_MS;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            STATE = lookup.findVarHandle(DefaultMqttConnection.class, "state", byte.class);
            LAST_PING_TIME = lookup.findVarHandle(DefaultMqttConnection.class, "lastPingTime", long.class);
            KEEP_ALIVE_TIMEOUT_MS = lookup.findVarHandle(DefaultMqttConnection.class, "keepAliveTimeoutMs", long.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final Connection connection;
    private final NettyOutbound outbound;

    private volatile String clientId = "unknown";
    private volatile MqttConnectMessage connectMessage;

    @SuppressWarnings("unused") // accessed via VarHandle
    private volatile byte state = STATE_INIT;
    @SuppressWarnings("unused") // accessed via VarHandle
    private volatile long lastPingTime;
    @SuppressWarnings("unused") // accessed via VarHandle
    private volatile long keepAliveTimeoutMs = 120_000L;

    private final Sinks.One<MqttConnectMessage> connectSink = Sinks.one();
    private final Sinks.Empty<Void> disposeSink = Sinks.empty();

    private volatile MqttMessageListener messageListener;
    private volatile boolean autoAck = true;

    private static final Duration CONNECTION_TIMEOUT = Duration.ofSeconds(10);

    public DefaultMqttConnection(Connection connection) {
        this.connection = connection;
        this.outbound = connection.outbound();
        LAST_PING_TIME.set(this, System.currentTimeMillis());

        connection.onDispose(() -> {
            if (casSetClosed()) {
                if (messageListener != null) {
                    messageListener.onDisconnect(this).subscribe();
                }
                emitEmpty(disposeSink);
            }
        });
    }

    /**
     * 启动连接处理流程
     *
     * @param handler 连接处理器
     * @return 连接完整生命周期的 Mono
     */
    public Mono<Void> run(Function<MqttConnection, Mono<Void>> handler) {
        return handleInbound()
                .mergeWith(awaitConnect()
                                   .flatMap(msg -> handler != null ? handler.apply(this) : accept())
                                   .onErrorResume(err -> close()))
                .then(onDispose());
    }

    /**
     * 启动消息处理流程（用于 handle 模式）
     */
    private Flux<Void> handleInbound() {
        return connection.inbound()
                         .receiveObject()
                         .onBackpressureDrop(drop -> System.out.println("drop = " + drop))
                         .cast(MqttMessage.class)
                         .concatMap(this::handleMqttMessageSync);
    }

    /**
     * CAS 设置关闭状态
     *
     * @return true 如果状态变更成功（之前未关闭）
     */
    private boolean casSetClosed() {
        byte current;
        byte next;
        do {
            current = (byte) STATE.get(this);
            if (isClosed(current)) {
                return false;
            }
            next = setClosed(current);
        } while (!STATE.compareAndSet(this, current, next));
        return true;
    }

    /**
     * CAS 设置接受状态
     *
     * @return true 如果状态变更成功（之前未接受）
     */
    private boolean casSetAccepted() {
        byte current;
        byte next;
        do {
            current = (byte) STATE.get(this);
            if (isAccepted(current)) {
                return false;
            }
            next = setAccepted(current);
        } while (!STATE.compareAndSet(this, current, next));
        return true;
    }

    private void emitEmpty(Sinks.Empty<Void> sink) {
        Sinks.EmitResult result = sink.tryEmitEmpty();
        if (result.isFailure() && log.isLoggable(Level.FINE)) {
            log.fine("Emit empty failed: " + result);
        }
    }

    private <T> void emitValue(Sinks.One<T> sink, T value) {
        Sinks.EmitResult result = sink.tryEmitValue(value);
        if (result.isFailure() && log.isLoggable(Level.FINE)) {
            log.fine("Emit value failed: " + result);
        }
    }

    private Mono<Void> handleMqttMessageSync(MqttMessage msg) {
        return Mono.defer(() -> {
            LAST_PING_TIME.set(this, System.currentTimeMillis());
            MqttMessageType type = msg.fixedHeader().messageType();

            if (type == MqttMessageType.CONNECT) {
                MqttConnectMessage connectMsg = (MqttConnectMessage) msg;
                this.connectMessage = connectMsg;
                this.clientId = connectMsg.payload().clientIdentifier();
                int keepAliveSeconds = connectMsg.variableHeader().keepAliveTimeSeconds();
                KEEP_ALIVE_TIMEOUT_MS.set(this, (keepAliveSeconds + 10) * 1000L);
                emitValue(connectSink, connectMsg);
                return Mono.empty();
            }

            if (!isAccepted((byte) STATE.get(this))) {
                return Mono.empty();
            }

            return switch (type) {
                case PUBLISH -> handlePublishSync((MqttPublishMessage) msg);
                case PUBREC -> handlePubRec((MqttMessageIdVariableHeader) msg.variableHeader());
                case PUBREL -> handlePubRel((MqttMessageIdVariableHeader) msg.variableHeader());
                case SUBSCRIBE -> handleSubscribeMsg((MqttSubscribeMessage) msg);
                case UNSUBSCRIBE -> handleUnsubscribeMsg((MqttUnsubscribeMessage) msg);
                case PINGREQ -> handlePingReq();
                case DISCONNECT -> close();
                default -> Mono.empty();
            };
        });
    }

    private Mono<Void> handlePublishSync(MqttPublishMessage msg) {
        if (messageListener == null) {
            ReferenceCountUtil.release(msg);
            return Mono.empty();
        }

        try {
            ReferenceCountUtil.retain(msg);
        } catch (Exception e) {
            log.warning("Failed to retain message: " + e.getMessage());
            ReferenceCountUtil.safeRelease(msg);
            return Mono.empty();
        }

        DefaultMqttPublishing publishing = new DefaultMqttPublishing(msg, clientId, this::send);

        if (msg.fixedHeader().qosLevel() != MqttQoS.AT_MOST_ONCE) {
            Mono<Void> handler = messageListener.onPublish(publishing);
            if (autoAck) {
                return handler.then(publishing.acknowledge())
                              .doFinally(signal -> publishing.release());
            } else {
                return handler.doFinally(signal -> publishing.release());
            }
        }
        return messageListener.onPublish(publishing)
                              .doFinally(signal -> publishing.release());
    }

    private Mono<Void> handleSubscribeMsg(MqttSubscribeMessage msg) {
        DefaultMqttSubscription sub = new DefaultMqttSubscription(msg, this::send);

        if (messageListener != null) {
            return messageListener.onSubscribe(sub)
                                  .then(Mono.defer(() -> Mono.from(sub.acknowledge())));
        }

        return Mono.from(sub.acknowledge());
    }

    private Mono<Void> handleUnsubscribeMsg(MqttUnsubscribeMessage msg) {
        DefaultMqttUnSubscription unsub = new DefaultMqttUnSubscription(msg, this::send);

        if (messageListener != null) {
            return messageListener.onUnsubscribe(unsub)
                                  .then(Mono.defer(() -> Mono.from(unsub.acknowledge())));
        }

        return Mono.from(unsub.acknowledge());
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

    private Mono<Void> send(Publisher<Object> msg) {
        return outbound.sendObject(msg).then();
    }

    public Mono<MqttConnectMessage> awaitConnect() {
        return connectSink.asMono().timeout(CONNECTION_TIMEOUT);
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
            if (isClosed((byte) STATE.get(this))) {
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
            if (!casSetAccepted()) {
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
    public MqttConnection listener(MqttMessageListener listener) {
        this.messageListener = listener;
        return this;
    }

    @Override
    public MqttConnection autoAck(boolean autoAck) {
        this.autoAck = autoAck;
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
        return !isClosed((byte) STATE.get(this)) && connection.channel().isActive();
    }

    @Override
    public Mono<Void> close() {
        return Mono.defer(() -> {
            if (!casSetClosed()) {
                return Mono.empty();
            }
            return Mono.fromRunnable(() -> {
                if (messageListener != null) {
                    messageListener.onDisconnect(this).subscribe();
                }
                emitEmpty(disposeSink);
                connection.dispose();
            });
        });
    }

    @Override
    public long getLastPingTime() {
        return (long) LAST_PING_TIME.get(this);
    }

    @Override
    public Duration getKeepAliveTimeout() {
        return Duration.ofMillis((long) KEEP_ALIVE_TIMEOUT_MS.get(this));
    }

    @Override
    public Mono<Void> setKeepAliveTimeout(Duration duration) {
        return Mono.fromRunnable(() -> KEEP_ALIVE_TIMEOUT_MS.set(this, duration.toMillis()));
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
