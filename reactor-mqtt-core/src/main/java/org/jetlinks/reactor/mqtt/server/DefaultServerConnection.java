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
import org.jetlinks.reactor.mqtt.Acknowledge;
import org.jetlinks.reactor.mqtt.MqttAuth;
import org.jetlinks.reactor.mqtt.MqttConstants;
import org.jetlinks.reactor.mqtt.MqttWillMessage;
import org.jetlinks.reactor.mqtt.Topic;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.Connection;
import reactor.netty.NettyInbound;
import reactor.netty.NettyOutbound;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
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
    private static final VarHandle KEEP_ALIVE_CHECK_TASK;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            STATE = lookup.findVarHandle(DefaultServerConnection.class, "state", byte.class);
            CLIENT_ID = lookup.findVarHandle(DefaultServerConnection.class, "clientId", String.class);
            CONNECT_MESSAGE = lookup.findVarHandle(DefaultServerConnection.class, "connectMessage", MqttConnectMessage.class);
            LAST_PING_TIME = lookup.findVarHandle(DefaultServerConnection.class, "lastPingTime", long.class);
            KEEP_ALIVE_TIMEOUT_MS = lookup.findVarHandle(DefaultServerConnection.class, "keepAliveTimeoutMs", long.class);
            PUBLISH_HANDLER = lookup.findVarHandle(DefaultServerConnection.class, "publishHandler", Function.class);
            SUBSCRIBE_HANDLER = lookup.findVarHandle(DefaultServerConnection.class, "subscribeHandler", Function.class);
            UNSUBSCRIBE_HANDLER = lookup.findVarHandle(DefaultServerConnection.class, "unsubscribeHandler", Function.class);
            AUTO_ACK = lookup.findVarHandle(DefaultServerConnection.class, "autoAck", boolean.class);
            MESSAGE_ID_GENERATOR = lookup.findVarHandle(DefaultServerConnection.class, "messageId", int.class);
            KEEP_ALIVE_CHECK_TASK = lookup.findVarHandle(DefaultServerConnection.class, "keepAliveCheckTask", Disposable.class);
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

    @SuppressWarnings("unused")
    private volatile Function<ServerReceivedPublish, Mono<Void>> publishHandler;

    @SuppressWarnings("unused")
    private volatile Function<MqttSubscription, Mono<Void>> subscribeHandler;

    @SuppressWarnings("unused")
    private volatile Function<MqttUnsubscription, Mono<Void>> unsubscribeHandler;

    @SuppressWarnings("unused")
    private volatile boolean autoAck = true;

    @SuppressWarnings("unused")
    private volatile int messageId = 0;
    @SuppressWarnings("unused")
    private volatile Disposable keepAliveCheckTask;

    private final Sinks.One<MqttConnectMessage> connectSink = Sinks.one();

    private final Sinks.Empty<Void> disposeSink = Sinks.empty();

    public DefaultServerConnection(NettyInbound inbound, NettyOutbound outbound) {
        this(inbound, outbound, true);
    }

    public DefaultServerConnection(NettyInbound inbound, NettyOutbound outbound, boolean autoAck) {
        this.inbound = inbound;
        this.outbound = outbound;
        this.connection = (Connection) inbound;
        AUTO_ACK.set(this, autoAck);
        LAST_PING_TIME.set(this, System.currentTimeMillis());
        startKeepAliveCheck();

        connection.onDispose(() -> {
            stopKeepAliveCheck();
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
        return Mono.when(
                        handleInbound(),
                        runConnectionHandler(handler)
                )
                .then(onClose());
    }

    /**
     * 启动消息处理流程（用于 handle 模式）
     */
    private Mono<Void> handleInbound() {
        return inbound
                .receiveObject()
                .cast(MqttMessage.class)
                .concatMap(this::handleMqttMessageSync)
                .then();
    }

    private Mono<Void> runConnectionHandler(Function<ServerConnection, Mono<Void>> handler) {
        Function<ServerConnection, Mono<Void>> actualHandler = handler != null ? handler : ignored -> accept();
        return awaitConnect()
                .then(Mono.defer(() -> actualHandler.apply(this)))
                .onErrorResume(error -> close());
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
        if (result.isFailure()) {
            log.log(Level.FINE, () -> "Emit empty failed: " + result);
        }
    }

    private <T> void emitValue(Sinks.One<T> sink, T value) {
        Sinks.EmitResult result = sink.tryEmitValue(value);
        if (result.isFailure()) {
            log.log(Level.FINE, () -> "Emit value failed: " + result);
        }
    }

    private void startKeepAliveCheck() {
        stopKeepAliveCheck();
        Disposable task = asDisposable(
                connection.channel()
                          .eventLoop()
                          .scheduleAtFixedRate(() -> {
                              if (!isAlive()) {
                                  return;
                              }
                              long now = System.currentTimeMillis();
                              long lastPing = getLastPingTime();
                              long timeout = (long) KEEP_ALIVE_TIMEOUT_MS.get(this);
                              if (now - lastPing > timeout) {
                                  log.log(Level.WARNING, () -> "Client " + CLIENT_ID.get(this) + " keepalive timeout, closing connection");
                                  connection.dispose();
                              }
                          }, 30, 30, TimeUnit.SECONDS)
        );
        KEEP_ALIVE_CHECK_TASK.set(this, task);
    }

    private void stopKeepAliveCheck() {
        Disposable task = (Disposable) KEEP_ALIVE_CHECK_TASK.getAndSet(this, null);
        if (task != null && !task.isDisposed()) {
            task.dispose();
        }
    }

    private Disposable asDisposable(io.netty.util.concurrent.Future<?> future) {
        return new Disposable() {
            @Override
            public void dispose() {
                future.cancel(true);
            }

            @Override
            public boolean isDisposed() {
                return future.isCancelled() || future.isDone();
            }
        };
    }

    private Mono<Void> handleMqttMessageSync(MqttMessage msg) {
        return Mono.defer(() -> {
            LAST_PING_TIME.set(this, System.currentTimeMillis());
            MqttMessageType type = msg.fixedHeader().messageType();

            if (type == MqttMessageType.CONNECT) {
                MqttConnectMessage connectMsg = (MqttConnectMessage) msg;
                CONNECT_MESSAGE.set(this, connectMsg);
                CLIENT_ID.set(this, connectMsg.payload().clientIdentifier());
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

    @SuppressWarnings("unchecked")
    private Mono<Void> handlePublishSync(MqttPublishMessage msg) {
        Function<ServerReceivedPublish, Mono<Void>> handler = currentPublishHandler();
        boolean shouldAutoAck = shouldAutoAck(msg);

        // 没有 handler 时，根据 autoAck 配置决定是否发送 ACK
        if (handler == null && shouldAutoAck) {
            return sendAck(msg);
        }

        try {
            ReferenceCountUtil.retain(msg);
        } catch (Exception e) {
            log.log(Level.WARNING, () -> "Failed to retain message: " + e.getMessage());
            ReferenceCountUtil.safeRelease(msg);
            return Mono.empty();
        }

        Topic topic = Topic.of(msg.variableHeader().topicName());
        DefaultServerReceivedPublish publishing = new DefaultServerReceivedPublish(msg, topic, this);

        return handlePublish(publishing, handler, shouldAutoAck);
    }

    private Mono<Void> sendAck(MqttPublishMessage msg) {
        int messageId = msg.variableHeader().packetId();
        MqttQoS qos = msg.fixedHeader().qosLevel();

        if (qos == MqttQoS.AT_LEAST_ONCE) {
            MqttMessage pubAck = MqttMessageBuilders.pubAck()
                                                    .packetId(messageId)
                                                    .build();
            return send(pubAck);
        } else if (qos == MqttQoS.EXACTLY_ONCE) {
            MqttMessage pubRec = new MqttMessage(
                    MqttConstants.MessageHeader.PUBREC_HEADER,
                    MqttMessageIdVariableHeader.from(messageId)
            );
            return send(pubRec);
        }
        return Mono.empty();
    }

    @SuppressWarnings("unchecked")
    private Mono<Void> handleSubscribeMsg(MqttSubscribeMessage msg) {
        DefaultMqttSubscription sub = new DefaultMqttSubscription(msg, this);
        return invokeHandlerAndAck(sub, currentSubscribeHandler());
    }

    @SuppressWarnings("unchecked")
    private Mono<Void> handleUnsubscribeMsg(MqttUnsubscribeMessage msg) {
        DefaultMqttUnsubscription unsub = new DefaultMqttUnsubscription(msg, this);
        return invokeHandlerAndAck(unsub, currentUnsubscribeHandler());
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

    public Mono<MqttConnectMessage> awaitConnect() {
        return connectSink.asMono().timeout(MqttConstants.Time.TEN_SECONDS);
    }

    @Override
    public String getClientId() {
        return (String) CLIENT_ID.get(this);
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return (InetSocketAddress) connection.channel().remoteAddress();
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) connection.channel().localAddress();
    }

    @Override
    public MqttVersion getVersion() {
        MqttConnectMessage msg = (MqttConnectMessage) CONNECT_MESSAGE.get(this);
        if (msg == null) {
            return MqttVersion.MQTT_3_1_1;
        }
        return MqttVersion.fromProtocolNameAndLevel(
                msg.variableHeader().name(),
                (byte) msg.variableHeader().version()
        );
    }

    @Override
    public MqttAuth getAuth() {
        MqttConnectMessage currentConnectMessage = (MqttConnectMessage) CONNECT_MESSAGE.get(this);
        if (currentConnectMessage == null || !currentConnectMessage.variableHeader().hasUserName()) {
            return MqttAuth.empty();
        }
        String username = currentConnectMessage.payload().userName();
        byte[] pwd = currentConnectMessage.payload().passwordInBytes();
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
                    .doOnSuccess(v -> log.log(Level.FINE, () -> "MQTT client [" + CLIENT_ID.get(this) + "] connected"));
        });
    }

    @Override
    public MqttWillMessage getWill() {
        MqttConnectMessage currentConnectMessage = (MqttConnectMessage) CONNECT_MESSAGE.get(this);
        if (currentConnectMessage == null || !currentConnectMessage.variableHeader().isWillFlag()) {
            return MqttWillMessage.EMPTY;
        }
        byte[] willPayload = currentConnectMessage.payload().willMessageInBytes();
        String topic = currentConnectMessage.payload().willTopic();
        ByteBuf payload = willPayload != null ? Unpooled.wrappedBuffer(willPayload) : null;
        MqttQoS qos = MqttQoS.valueOf(currentConnectMessage.variableHeader().willQos());
        boolean retain = currentConnectMessage.variableHeader().isWillRetain();
        return new MqttWillMessage(topic, payload, qos, retain, MqttProperties.NO_PROPERTIES);
    }

    @Override
    public ServerConnection handlePublishing(Function<ServerReceivedPublish, Mono<Void>> handler) {
        PUBLISH_HANDLER.set(this, handler);
        return this;
    }

    @Override
    public ServerConnection handleSubscribe(Function<MqttSubscription, Mono<Void>> handler) {
        SUBSCRIBE_HANDLER.set(this, handler);
        return this;
    }

    @Override
    public ServerConnection handleUnsubscribe(Function<MqttUnsubscription, Mono<Void>> handler) {
        UNSUBSCRIBE_HANDLER.set(this, handler);
        return this;
    }

    @Override
    public ServerConnection autoAck(boolean autoAck) {
        AUTO_ACK.set(this, autoAck);
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
                stopKeepAliveCheck();
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

    private Mono<Void> handlePublish(DefaultServerReceivedPublish publishing,
                                     Function<ServerReceivedPublish, Mono<Void>> handler,
                                     boolean shouldAutoAck) {
        Mono<Void> task = invokeHandler(publishing, handler);
        if (shouldAutoAck) {
            task = task.then(publishing.ack());
        }
        return task.doFinally(signal -> publishing.release());
    }

    private <T> Mono<Void> invokeHandler(T value, Function<T, Mono<Void>> handler) {
        return handler == null ? Mono.empty() : Mono.defer(() -> handler.apply(value));
    }

    private <T extends Acknowledge> Mono<Void> invokeHandlerAndAck(T value, Function<T, Mono<Void>> handler) {
        return invokeHandler(value, handler).then(value.ack());
    }

    @SuppressWarnings("unchecked")
    private Function<ServerReceivedPublish, Mono<Void>> currentPublishHandler() {
        return (Function<ServerReceivedPublish, Mono<Void>>) PUBLISH_HANDLER.get(this);
    }

    private boolean shouldAutoAck(MqttPublishMessage msg) {
        return (boolean) AUTO_ACK.get(this) && msg.fixedHeader().qosLevel() != MqttQoS.AT_MOST_ONCE;
    }

    @SuppressWarnings("unchecked")
    private Function<MqttSubscription, Mono<Void>> currentSubscribeHandler() {
        return (Function<MqttSubscription, Mono<Void>>) SUBSCRIBE_HANDLER.get(this);
    }

    @SuppressWarnings("unchecked")
    private Function<MqttUnsubscription, Mono<Void>> currentUnsubscribeHandler() {
        return (Function<MqttUnsubscription, Mono<Void>>) UNSUBSCRIBE_HANDLER.get(this);
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
