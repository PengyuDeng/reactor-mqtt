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
import org.jetlinks.reactor.mqtt.MqttAuth;
import org.jetlinks.reactor.mqtt.MqttWillMessage;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.Connection;
import reactor.netty.NettyInbound;
import reactor.netty.NettyOutbound;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.jetlinks.reactor.mqtt.MqttConstants.MessageHeader.PUBCOMP_HEADER;
import static org.jetlinks.reactor.mqtt.MqttConstants.MessageHeader.PUBREL_HEADER;

/**
 * 基于 Reactor Netty 的 MQTT 连接实现 - 纯响应式
 *
 * @author PengyuDeng
 */
public class DefaultServerConnection implements ServerConnection {

    private static final Logger log = Logger.getLogger(DefaultServerConnection.class.getName());

    private static final VarHandle STATE;
    private static final VarHandle CLIENT_ID;
    private static final VarHandle CONNECT_MESSAGE;
    private static final VarHandle LAST_PING_TIME;
    private static final VarHandle KEEP_ALIVE_TIMEOUT_MS;
    private static final VarHandle PUBLISH_HANDLER;
    private static final VarHandle SUBSCRIBE_HANDLER;
    private static final VarHandle UNSUBSCRIBE_HANDLER;
    private static final VarHandle AUTO_ACK;
    private static final VarHandle MESSAGE_ID_GENERATOR;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            STATE = lookup.findVarHandle(DefaultServerConnection.class, "state", byte.class);
            CLIENT_ID = lookup.findVarHandle(DefaultServerConnection.class, "clientId", String.class);
            CONNECT_MESSAGE = lookup.findVarHandle(DefaultServerConnection.class, "connectMessage", MqttConnectMessage.class);
            LAST_PING_TIME = lookup.findVarHandle(DefaultServerConnection.class, "lastPingTime", long.class);
            KEEP_ALIVE_TIMEOUT_MS = lookup.findVarHandle(DefaultServerConnection.class, "keepAliveTimeoutMs", long.class);
            PUBLISH_HANDLER = lookup.findVarHandle(DefaultServerConnection.class, "publishHandler", Consumer.class);
            SUBSCRIBE_HANDLER = lookup.findVarHandle(DefaultServerConnection.class, "subscribeHandler", Consumer.class);
            UNSUBSCRIBE_HANDLER = lookup.findVarHandle(DefaultServerConnection.class, "unsubscribeHandler", Consumer.class);
            AUTO_ACK = lookup.findVarHandle(DefaultServerConnection.class, "autoAck", boolean.class);
            MESSAGE_ID_GENERATOR = lookup.findVarHandle(DefaultServerConnection.class, "messageId", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final Connection connection;
    private final NettyInbound inbound;
    private final NettyOutbound outbound;

    @SuppressWarnings("unused")
    private volatile String clientId = "unknown";
    @SuppressWarnings("unused")
    private volatile MqttConnectMessage connectMessage;

    @SuppressWarnings("unused")
    private volatile byte state = State.INIT;
    @SuppressWarnings("unused")
    private volatile long lastPingTime;
    @SuppressWarnings("unused")
    private volatile long keepAliveTimeoutMs = 120_000L;

    private final Sinks.One<MqttConnectMessage> connectSink = Sinks.one();
    private final Sinks.Empty<Void> disposeSink = Sinks.empty();

    @SuppressWarnings("unused")
    private volatile Consumer<ServerReceivedPublish> publishHandler;
    @SuppressWarnings("unused")
    private volatile Consumer<MqttSubscription> subscribeHandler;
    @SuppressWarnings("unused")
    private volatile Consumer<MqttUnsubscription> unsubscribeHandler;
    @SuppressWarnings("unused")
    private volatile boolean autoAck = true;

    @SuppressWarnings("unused")
    private volatile int messageId = 0;

    private static final Duration CONNECTION_TIMEOUT = Duration.ofSeconds(10);

    public DefaultServerConnection(NettyInbound inbound, NettyOutbound outbound) {
        this.inbound = inbound;
        this.outbound = outbound;
        this.connection = (Connection) inbound;
        LAST_PING_TIME.set(this, System.currentTimeMillis());

        // 启动 KeepAlive 超时检测
        Flux.interval(Duration.ofSeconds(30))
            .takeUntil(v -> !isAlive())
            .subscribe(tick -> {
                long now = System.currentTimeMillis();
                long lastPing = getLastPingTime();
                long timeout = (long) KEEP_ALIVE_TIMEOUT_MS.get(this);

                if (now - lastPing > timeout) {
                    log.warning("Client " + clientId + " keepalive timeout, closing connection");
                    close().subscribe();
                }
            });

        connection.onDispose(() -> {
            if (casSetClosed()) {
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
    public Mono<Void> run(Function<ServerConnection, Mono<Void>> handler) {
        return handleInbound()
                .mergeWith(awaitConnect()
                                   .flatMap(msg -> handler != null ? handler.apply(this) : accept())
                                   .onErrorResume(err -> close()))
                .then(onClose());
    }

    /**
     * 启动消息处理流程（用于 handle 模式）
     */
    private Flux<Void> handleInbound() {
        return inbound
                .receiveObject()
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
            if (State.isClosed(current)) {
                return false;
            }
            next = State.setClosed(current);
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
            if (State.isAccepted(current)) {
                return false;
            }
            next = State.setAccepted(current);
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

            if (!State.isAccepted((byte) STATE.get(this))) {
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
        // 如果有自定义的publish handler，调用它
        if (publishHandler != null) {
            try {
                ReferenceCountUtil.retain(msg);
            } catch (Exception e) {
                log.warning("Failed to retain message: " + e.getMessage());
                ReferenceCountUtil.safeRelease(msg);
                return Mono.empty();
            }

            DefaultServerReceivedPublish publishing = new DefaultServerReceivedPublish(msg, clientId, this::send);

            Mono<Void> handler = Mono.fromRunnable(() -> publishHandler.accept(publishing));

            if (msg.fixedHeader().qosLevel() != MqttQoS.AT_MOST_ONCE) {
                if (autoAck) {
                    return handler.then(publishing.acknowledge())
                                  .doFinally(signal -> publishing.release());
                } else {
                    return handler.doFinally(signal -> publishing.release());
                }
            }
            return handler.doFinally(signal -> publishing.release());
        }

        return Mono.empty();
    }

    private Mono<Void> handleSubscribeMsg(MqttSubscribeMessage msg) {
        DefaultMqttSubscription sub = new DefaultMqttSubscription(msg, this);

        if (subscribeHandler != null) {
            return Mono.fromRunnable(() -> subscribeHandler.accept(sub))
                       .then(Mono.defer(() -> Mono.from(sub.acknowledge())));
        }

        return Mono.from(sub.acknowledge());
    }

    private Mono<Void> handleUnsubscribeMsg(MqttUnsubscribeMessage msg) {
        DefaultMqttUnsubscription unsub = new DefaultMqttUnsubscription(msg, this);

        if (unsubscribeHandler != null) {
            return Mono.fromRunnable(() -> unsubscribeHandler.accept(unsub))
                       .then(Mono.defer(() -> Mono.from(unsub.acknowledge())));
        }

        return Mono.from(unsub.acknowledge());
    }

    private Mono<Void> handlePubRec(MqttMessageIdVariableHeader header) {
        MqttMessage pubRel = new MqttMessage(
                PUBREL_HEADER,
                MqttMessageIdVariableHeader.from(header.messageId())
        );
        return send(pubRel);
    }

    private Mono<Void> handlePubRel(MqttMessageIdVariableHeader header) {
        MqttMessage pubComp = new MqttMessage(
                PUBCOMP_HEADER,
                MqttMessageIdVariableHeader.from(header.messageId())
        );
        return send(pubComp);
    }

    private Mono<Void> handlePingReq() {
        return send(MqttMessage.PINGRESP);
    }

    Mono<Void> send(MqttMessage msg) {
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
            if (State.isClosed((byte) STATE.get(this))) {
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
    public MqttWillMessage getWill() {
        if (connectMessage == null || !connectMessage.variableHeader().isWillFlag()) {
            return MqttWillMessage.EMPTY;
        }
        byte[] willPayload = connectMessage.payload().willMessageInBytes();
        String topic = connectMessage.payload().willTopic();
        ByteBuf payload = willPayload != null ? Unpooled.wrappedBuffer(willPayload) : null;
        MqttQoS qos = MqttQoS.valueOf(connectMessage.variableHeader().willQos());
        boolean retain = connectMessage.variableHeader().isWillRetain();
        return new MqttWillMessage(topic, payload, qos, retain, MqttProperties.NO_PROPERTIES);
    }

    @Override
    public ServerConnection handlePublishing(Consumer<ServerReceivedPublish> message) {
        this.publishHandler = message;
        return this;
    }

    @Override
    public ServerConnection onSubscribe(Consumer<MqttSubscription> subscription) {
        this.subscribeHandler = subscription;
        return this;
    }

    @Override
    public ServerConnection onUnsubscribe(Consumer<MqttUnsubscription> unsubscription) {
        this.unsubscribeHandler = unsubscription;
        return this;
    }

    @Override
    public ServerConnection autoAck(boolean autoAck) {
        this.autoAck = autoAck;
        return this;
    }

    @Override
    public Mono<Void> publish(MqttPublishMessage message) {
        return Mono.defer(() -> {
            MqttFixedHeader fixedHeader = message.fixedHeader();
            MqttQoS qos = fixedHeader.qosLevel();

            if (qos == MqttQoS.AT_MOST_ONCE) {
                return send(message);
            }

            int currentMessageId = message.variableHeader().packetId();

            if (currentMessageId > 0 && currentMessageId <= 65535) {
                return send(message);
            }

            int newMessageId = nextMessageId();

            MqttPublishMessage newMessage = MqttMessageBuilders.publish()
                                                               .topicName(message.variableHeader().topicName())
                                                               .payload(message.payload().retain())
                                                               .qos(qos)
                                                               .retained(fixedHeader.isRetain())
                                                               .messageId(newMessageId)
                                                               .build();

            return send(newMessage);
        });
    }

    /**
     * 发布消息的便捷方法（用于broker内部调用）
     *
     * @param topic   主题
     * @param payload 消息体
     * @param qos     QoS级别
     * @param retain  是否保留
     * @return 发布完成的Mono
     */
    public Mono<Void> publish(String topic, ByteBuf payload, MqttQoS qos, boolean retain) {
        int messageId = qos == MqttQoS.AT_MOST_ONCE ? 0 : nextMessageId();

        MqttPublishMessage publishMessage = MqttMessageBuilders.publish()
                                                               .topicName(topic)
                                                               .payload(payload)
                                                               .qos(qos)
                                                               .retained(retain)
                                                               .messageId(messageId)
                                                               .build();

        return publish(publishMessage);
    }

    /**
     * 生成下一个 MQTT 消息 ID,范围 1-65535
     * 通过 (id % 65535) + 1 确保ID在 1-65535 范围内循环，无重复。</p>
     *
     * @return 消息 ID (1-65535)
     */
    private int nextMessageId() {
        int id = (int) MESSAGE_ID_GENERATOR.getAndAdd(this, 1);
        return ((id & 0x7FFFFFFF) % 65535) + 1;
    }

    @Override
    public Mono<Void> onClose() {
        return disposeSink.asMono();
    }

    @Override
    public boolean isAlive() {
        return !State.isClosed((byte) STATE.get(this)) && connection.channel().isActive();
    }

    @Override
    public Mono<Void> close() {
        return Mono.defer(() -> {
            if (!casSetClosed()) {
                return Mono.empty();
            }
            return Mono.fromRunnable(() -> {
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

    /**
     * MQTT 连接状态常量
     *
     * <p>使用位掩码管理连接状态，支持多状态组合。</p>
     *
     * <pre>
     * 状态位布局 (byte):
     * bit 0: CLOSED     - 连接已关闭
     * bit 1: ACCEPTED   - 连接已被接受
     * bit 2-7: 保留
     * </pre>
     */
    private interface State {
        byte INIT = 0;
        byte CLOSED = 1;
        byte ACCEPTED = 1 << 1;

        static boolean isClosed(byte state) {
            return (state & CLOSED) != 0;
        }

        static boolean isAccepted(byte state) {
            return (state & ACCEPTED) != 0;
        }

        static byte setClosed(byte state) {
            return (byte) (state | CLOSED);
        }

        static byte setAccepted(byte state) {
            return (byte) (state | ACCEPTED);
        }
    }
}
