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
import io.netty.handler.codec.mqtt.*;
import io.netty.util.ReferenceCountUtil;
import org.jetlinks.reactor.mqtt.MqttWillMessage;
import org.jetlinks.reactor.mqtt.Topic;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.jetlinks.reactor.mqtt.MqttConstants.MessageHeader.PUBCOMP_HEADER;
import static org.jetlinks.reactor.mqtt.MqttConstants.MessageHeader.PUBREL_HEADER;

/**
 * MQTT 客户端连接实现
 *
 * @author PengyuDeng
 */
public class DefaultClientConnection implements ClientConnection {

    private static final Logger log = Logger.getLogger(DefaultClientConnection.class.getName());


    /**
     * 客户端配置(引用,支持动态修改,重连时生效)
     *
     * <p>注意: 配置在连接建立后不应该被修改,除非希望在重连时生效。
     * 详见 {@link MqttClientConfig} 的约定说明。</p>
     */
    private final MqttClientConfig config;

    /**
     * TCP 客户端提供者,用于创建新连接
     */
    private final Supplier<TcpClient> tcpClientSupplier;

    /**
     * Netty TCP 连接,使用 VarHandle 保证重连时的可见性
     */
    @SuppressWarnings("unused")
    private volatile Connection connection;

    /**
     * 状态字段: bit0-2=状态标志, bit3-31=重连次数
     * 标志位: bit0=connected, bit1=closed, bit2=reconnecting
     * state 布局:
     * ┌─────────────────────────────────┬────┬────┬────┐
     * │  重连次数 (29 bits)              │ R  │ C  │ N  │
     * │  bit 3-31                       │bit2│bit1│bit0│
     * └─────────────────────────────────┴────┴────┴────┘
     * N = CONNECTED (1)
     * C = CLOSED (2)
     * R = RECONNECTING (4)
     */
    @SuppressWarnings("unused")
    private volatile int state = 0;

    private static final VarHandle STATE;
    private static final VarHandle MESSAGE_ID_GENERATOR;
    private static final VarHandle CONNECTION;
    private static final VarHandle CONN_ACK_SINK;
    private static final VarHandle HEARTBEAT_TIMER;
    private static final VarHandle RECONNECT_TASK;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            STATE = lookup.findVarHandle(DefaultClientConnection.class, "state", int.class);
            MESSAGE_ID_GENERATOR = lookup.findVarHandle(DefaultClientConnection.class, "messageIdGenerator", int.class);
            CONNECTION = lookup.findVarHandle(DefaultClientConnection.class, "connection", Connection.class);
            CONN_ACK_SINK = lookup.findVarHandle(DefaultClientConnection.class, "connAckSink", Sinks.One.class);
            HEARTBEAT_TIMER = lookup.findVarHandle(DefaultClientConnection.class, "heartbeatTimer", Disposable.class);
            RECONNECT_TASK = lookup.findVarHandle(DefaultClientConnection.class, "reconnectTask", Disposable.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // 状态标志位 (bit 0-2)
    private static final int CONNECTED = 1;
    private static final int CLOSED = 2;
    private static final int RECONNECTING = 4;
    // 低3位
    private static final int FLAGS_MASK = 0x7;
    // 重连次数从 bit3 开始
    private static final int ATTEMPT_INCREMENT = 0x8;

    /**
     * 关闭完成信号,用于 onClose() 方法
     */
    private final Sinks.Empty<Void> closeSink = Sinks.empty();

    /**
     * CONNACK 消息接收器,每次重连时重置
     */
    private volatile Sinks.One<MqttConnAckMessage> connAckSink;

    /**
     * 重连成功信号发射器,用于 onReconnect() 方法
     * 发射重连次数
     */
    private final Sinks.Many<Integer> reconnectSink = Sinks.many().multicast().onBackpressureBuffer();

    /**
     * MQTT 消息 ID 生成器,使用 VarHandle 保证原子性和可见性
     * 范围 1-65535,循环使用
     */
    @SuppressWarnings("unused")
    private volatile int messageIdGenerator = 0;

    /**
     * 统一的 pending 消息映射: key = (MqttMessageType.ordinal << 16) | messageId
     * 使用小容量初始化以节省内存（大多数连接不会有很多并发消息）
     */
    private final Map<Integer, Sinks.Empty<Void>> pendingAcks = new ConcurrentHashMap<>(4, 0.75f, 2);

    /**
     * 订阅管理器,统一管理订阅和处理器
     */
    private final SubscriptionManager subscriptionManager;

    /**
     * 心跳定时器
     */
    private volatile Disposable heartbeatTimer;

    /**
     * 重连任务句柄,用于在 close() 时取消后台重连
     */
    private volatile Disposable reconnectTask;

    public DefaultClientConnection(Connection connection,
                                   MqttClientConfig clientConfig,
                                   Supplier<TcpClient> tcpClientSupplier) {

        CONNECTION.set(this, connection);
        this.config = clientConfig;
        this.tcpClientSupplier = tcpClientSupplier;
        this.subscriptionManager = clientConfig.getSubscriptionManager() != null
                ? clientConfig.getSubscriptionManager()
                : SubscriptionManager.create();
    }

    private static int pendingKey(MqttMessageType type, int messageId) {
        return (type.value() << 16) | messageId;
    }

    /**
     * 初始化连接（发送 CONNECT，等待 CONNACK）
     */
    Mono<ClientConnection> initialize() {
        return Mono.defer(() -> {
            setConnAckSink(Sinks.one());
            return ensureMqttCodec()
                    .then(Mono.fromRunnable(this::setupConnectionHandlers))
                    .then(sendConnect())
                    .doOnSuccess(v -> startHeartbeat())
                    .thenReturn(this);
        });
    }

    private Mono<Void> ensureMqttCodec() {
        return Mono.fromRunnable(() -> {
                       Connection conn = (Connection) CONNECTION.get(this);
                       if (conn.channel().pipeline().get("mqttEncoder") == null) {
                           conn.addHandlerFirst("mqttEncoder", MqttEncoder.INSTANCE);
                       }
                       if (conn.channel().pipeline().get("mqttDecoder") == null) {
                           conn.addHandlerFirst("mqttDecoder", new MqttDecoder(config.getMaxMessageSize()));
                       }
                   })
                   .then()
                   .onErrorResume(e -> {
                       log.log(Level.WARNING, e, () -> "Failed to add MQTT codec: " + e.getMessage());
                       return Mono.empty();
                   });
    }

    private void setupConnectionHandlers() {
        Connection conn = (Connection) CONNECTION.get(this);
        ReactiveTaskSupport.ManagedTask inboundTask = ReactiveTaskSupport.create(
                this::handleComplete,
                this::handleError
        );
        conn.onDispose(inboundTask);
        inboundTask.start(handleInboundMessages(
                conn.inbound()
                    .receiveObject()
                    .cast(MqttMessage.class)
        ));

        conn.onDispose(() -> {
            if (!hasFlag(CLOSED)) {
                handleDisconnect();
            }
        });
    }

    Mono<Void> handleInboundMessages(Flux<MqttMessage> inboundMessages) {
        return inboundMessages.flatMap(this::handleMessage).then();
    }

    private Mono<Void> sendConnect() {
        MqttMessageBuilders.ConnectBuilder builder = MqttMessageBuilders.connect()
                                                                        .clientId(config.getClientId())
                                                                        .keepAlive(config.getKeepAlive())
                                                                        .cleanSession(config.isCleanSession());

        if (config.getProtocolVersion() == MqttVersion.MQTT_5) {
            builder.protocolVersion(MqttVersion.MQTT_5);
        }

        if (config.getUsername() != null) {
            builder.username(config.getUsername());
            if (config.getPassword() != null) {
                builder.password(config.getPassword());
            }
        }

        MqttWillMessage willMessage = config.getWillMessage();
        if (willMessage != null && willMessage.hasWill()) {
            builder.willTopic(willMessage.topic())
                   .willMessage(willMessage.payloadBytes())
                   .willQoS(willMessage.qos())
                   .willRetain(willMessage.retain());
        }

        MqttConnectMessage connectMessage = builder.build();

        return send(connectMessage)
                .then(currentConnAckSink().asMono()
                                 .timeout(config.getConnectTimeout())
                                 .flatMap(connAck -> {
                                      if (connAck
                                              .variableHeader()
                                             .connectReturnCode() == MqttConnectReturnCode.CONNECTION_ACCEPTED) {
                                         setConnectedFlag();
                                         resetReconnectAttempt();
                                         startHeartbeat();
                                         return Mono.empty();
                                     } else {
                                         return Mono.error(new MqttConnectionException(
                                                 "Connection refused: " + connAck
                                                         .variableHeader()
                                                         .connectReturnCode()));
                                     }
                                 }));
    }

    private Mono<Void> handleMessage(MqttMessage msg) {
        MqttMessageType type = msg.fixedHeader().messageType();

        return switch (type) {
            case CONNACK -> handleConnAck((MqttConnAckMessage) msg);
            case PUBLISH -> handlePublish((MqttPublishMessage) msg);
            case PUBACK -> handlePubAck((MqttPubAckMessage) msg);
            case PUBREC -> handlePubRec(msg);
            case PUBREL -> handlePubRel(msg);
            case PUBCOMP -> handlePubComp(msg);
            case SUBACK -> handleSubAck((MqttSubAckMessage) msg);
            case UNSUBACK -> handleUnsubAck(msg);
            case PINGRESP -> Mono.empty();
            default -> Mono.empty();
        };
    }

    private Mono<Void> handleConnAck(MqttConnAckMessage msg) {
        Sinks.EmitResult result = currentConnAckSink().tryEmitValue(msg);
        if (result.isFailure()) {
            log.log(Level.WARNING, () -> "Failed to emit CONNACK: " + result + ", this should not happen after reconnect fix");
        }
        return Mono.empty();
    }

    private Mono<Void> handlePublish(MqttPublishMessage msg) {
        try {
            ReferenceCountUtil.retain(msg);
        } catch (Exception e) {
            return Mono.empty();
        }

        Topic topic = Topic.of(msg.variableHeader().topicName());
        DefaultClientReceivedPublish publishing = new DefaultClientReceivedPublish(msg, topic, this);

        // 委托给 SubscriptionManager 处理订阅匹配
        Mono<Void> handlerMono = subscriptionManager.handleMessage(publishing);

        // 全局处理器
        Function<ClientReceivedPublish, Mono<Void>> publishingHandler = config.getPublishingHandler();
        if (publishingHandler != null) {
            handlerMono = handlerMono.then(publishingHandler.apply(publishing));
        }

        // 自动确认
        if (config.isAutoAck() && msg.fixedHeader().qosLevel() != MqttQoS.AT_MOST_ONCE) {
            handlerMono = handlerMono.then(publishing.ack());
        }

        return handlerMono.doFinally(signal -> publishing.release())
                          .doOnError(error -> log.log(Level.SEVERE, "  Handler error", error));
    }

    private Mono<Void> handlePubAck(MqttPubAckMessage msg) {
        int messageId = msg.variableHeader().messageId();
        Sinks.Empty<Void> sink = pendingAcks.remove(pendingKey(MqttMessageType.PUBACK, messageId));
        if (sink != null) {
            sink.tryEmitEmpty();
        }
        return Mono.empty();
    }

    private Mono<Void> handlePubRec(MqttMessage msg) {
        int messageId = ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();
        Sinks.Empty<Void> sink = pendingAcks.remove(pendingKey(MqttMessageType.PUBREC, messageId));
        if (sink != null) {
            sink.tryEmitEmpty();
        }

        MqttMessage pubRel = new MqttMessage(
                PUBREL_HEADER,
                MqttMessageIdVariableHeader.from(messageId)
        );

        Sinks.Empty<Void> compSink = Sinks.empty();
        pendingAcks.put(pendingKey(MqttMessageType.PUBCOMP, messageId), compSink);

        return send(pubRel);
    }

    private Mono<Void> handlePubRel(MqttMessage msg) {
        int messageId = ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();

        MqttMessage pubComp = new MqttMessage(
                PUBCOMP_HEADER,
                MqttMessageIdVariableHeader.from(messageId)
        );

        return send(pubComp);
    }

    private Mono<Void> handlePubComp(MqttMessage msg) {
        int messageId = ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();
        Sinks.Empty<Void> sink = pendingAcks.remove(pendingKey(MqttMessageType.PUBCOMP, messageId));
        if (sink != null) {
            sink.tryEmitEmpty();
        }
        return Mono.empty();
    }

    private Mono<Void> handleSubAck(MqttSubAckMessage msg) {
        int messageId = msg.variableHeader().messageId();
        Sinks.Empty<Void> sink = pendingAcks.remove(pendingKey(MqttMessageType.SUBACK, messageId));
        if (sink != null) {
            sink.tryEmitEmpty();
        }
        return Mono.empty();
    }

    private Mono<Void> handleUnsubAck(MqttMessage msg) {
        int messageId = ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();
        Sinks.Empty<Void> sink = pendingAcks.remove(pendingKey(MqttMessageType.UNSUBACK, messageId));
        if (sink != null) {
            sink.tryEmitEmpty();
        }
        return Mono.empty();
    }

    private void handleError(Throwable error) {
        log.log(Level.WARNING, error, () -> "Connection error for client " + config.getClientId() + ": " + error.getMessage());
        handleDisconnect();
    }

    private void handleComplete() {
        handleDisconnect();
    }

    private void handleDisconnect() {
        stopHeartbeat();
        if (!clearFlag(CONNECTED)) {
            return;
        }

        if (hasFlag(CLOSED)) {
            closeSink.tryEmitEmpty();
            return;
        }

        // 尝试重连
        attemptReconnect();
    }

    private void attemptReconnect() {
        if (hasFlag(CLOSED)) {
            return;
        }

        if (flagAlreadySet(RECONNECTING)) {
            return;
        }

        int attempt = incrementReconnectAttempt();

        setConnAckSink(Sinks.one());

        Mono<Void> reconnect = config.getReconnectStrategy()
                                     .nextDelay(attempt, null)
                                     .flatMap(this::reconnectAfterDelay)
                                     .switchIfEmpty(closeWhenReconnectExhausted());

        ReactiveTaskSupport.ManagedTask[] holder = new ReactiveTaskSupport.ManagedTask[1];
        ReactiveTaskSupport.ManagedTask reconnectTask = ReactiveTaskSupport.create(
                () -> onReconnectComplete(attempt, holder[0]),
                error -> onReconnectError(holder[0], error)
        );
        holder[0] = reconnectTask;

        if (!RECONNECT_TASK.compareAndSet(this, null, reconnectTask)) {
            if (!reconnectTask.isDisposed()) {
                reconnectTask.dispose();
            }
            return;
        }
        reconnectTask.start(reconnect);

        if (hasFlag(CLOSED)) {
            cancelReconnect();
        }
    }

    private Mono<Void> reconnectAfterDelay(Duration delay) {
        return Mono.delay(delay)
                   .flatMap(ignored -> reconnectIfOpen());
    }

    private Mono<Void> reconnectIfOpen() {
        return Mono.defer(() -> {
            if (hasFlag(CLOSED)) {
                return Mono.empty();
            }
            return tcpClientSupplier.get()
                                    .connect()
                                    .flatMap(this::initializeReconnectedConnection);
        });
    }

    private Mono<Void> initializeReconnectedConnection(Connection conn) {
        CONNECTION.set(this, conn);
        return ensureMqttCodec()
                .then(Mono.fromRunnable(this::setupConnectionHandlers))
                .then(sendConnect())
                .then(resubscribeIfNeeded());
    }

    private Mono<Void> closeWhenReconnectExhausted() {
        return Mono.defer(() -> {
            log.log(Level.WARNING, () -> "Max reconnect attempts reached for client " + config.getClientId() + ", closing connection");
            clearFlag(RECONNECTING);
            return close();
        });
    }

    private Mono<Void> resubscribeIfNeeded() {
        if (!config.isAutoResubscribe()) {
            return Mono.empty();
        }

        return ReactiveTaskSupport.whenAll(
                subscriptionManager.getSubscriptions(),
                sub -> doSubscribe(sub.topic(), sub.qos())
        );
    }

    @Override
    public Mono<Void> publish(MqttPublishMessage message) {
        return Mono.defer(() -> {
            if (!hasFlag(CONNECTED)) {
                return Mono.error(new IllegalStateException("Not connected"));
            }
            MqttQoS qos = message.fixedHeader().qosLevel();
            int originalMessageId = message.variableHeader().packetId();

            // 如果消息 ID 为 0 且 QoS > 0，需要生成新的消息 ID
            final int messageId;
            final MqttPublishMessage finalMessage;
            if (originalMessageId == 0 && qos != MqttQoS.AT_MOST_ONCE) {
                messageId = nextMessageId();
                finalMessage = MqttMessageBuilders.publish()
                                                  .topicName(message.variableHeader().topicName())
                                                  .payload(message.payload())
                                                  .qos(qos)
                                                  .retained(message.fixedHeader().isRetain())
                                                  .messageId(messageId)
                                                  .build();
            } else {
                messageId = originalMessageId;
                finalMessage = message;
            }

            if (qos == MqttQoS.AT_MOST_ONCE) {
                return send(finalMessage);
            } else if (qos == MqttQoS.AT_LEAST_ONCE) {
                return sendAndAwaitAck(
                        finalMessage,
                        pendingKey(MqttMessageType.PUBACK, messageId),
                        config.getPublishTimeout()
                );
            } else {
                // QoS 2
                int pubRecKey = pendingKey(MqttMessageType.PUBREC, messageId);
                int pubCompKey = pendingKey(MqttMessageType.PUBCOMP, messageId);
                registerPendingAck(pubRecKey);
                return send(finalMessage)
                        .then(awaitPendingAck(pubRecKey))
                        .then(Mono.defer(() -> awaitPendingAck(pubCompKey)))
                        .timeout(config.getPublishTimeout())
                        .doOnError(e -> {
                            pendingAcks.remove(pubRecKey);
                            pendingAcks.remove(pubCompKey);
                        });
            }
        });
    }

    @Override
    public Mono<Void> publish(String topic, ByteBuf payload, MqttQoS qos, boolean retain) {
        int messageId = qos == MqttQoS.AT_MOST_ONCE ? 0 : nextMessageId();

        MqttPublishMessage publishMessage = MqttMessageBuilders.publish()
                                                               .topicName(topic)
                                                               .payload(payload != null ? payload : Unpooled.EMPTY_BUFFER)
                                                               .qos(qos)
                                                               .retained(retain)
                                                               .messageId(messageId)
                                                               .build();

        return publish(publishMessage);
    }

    @Override
    public Disposable subscribe(String topic, MqttQoS qos, Function<ClientReceivedPublish, Mono<Void>> handler) {
        return subscriptionManager.subscribe(this, topic, qos, handler);
    }

    @Override
    public Disposable subscribe(Collection<String> topics, MqttQoS qos, Function<ClientReceivedPublish, Mono<Void>> handler) {
        // 为每个主题创建订阅，并返回一个组合的Disposable
        Disposable.Composite composite = Disposables.composite();
        for (String topic : topics) {
            composite.add(subscriptionManager.subscribe(this, topic, qos, handler));
        }
        return composite;
    }


    /**
     * 内部订阅方法,支持指定 QoS,供 SubscriptionManager 使用
     *
     * @param topic 订阅主题
     * @param qos   QoS 级别
     * @return 订阅完成的 Mono
     */
    Mono<Void> doSubscribe(String topic, MqttQoS qos) {
        int messageId = nextMessageId();

        MqttSubscribeMessage subscribeMessage = MqttMessageBuilders.subscribe()
                                                                   .messageId(messageId)
                                                                   .addSubscription(qos, topic)
                                                                   .build();

        return sendAndAwaitAck(
                subscribeMessage,
                pendingKey(MqttMessageType.SUBACK, messageId),
                config.getSubscribeTimeout()
        );
    }


    @Override
    public Mono<Void> unsubscribe(String... topics) {
        if (topics == null || topics.length == 0) {
            return Mono.empty();
        }

        int messageId = nextMessageId();

        MqttMessageBuilders.UnsubscribeBuilder unsubscribeBuilder = MqttMessageBuilders.unsubscribe()
                                                                                       .messageId(messageId);

        for (String topic : topics) {
            unsubscribeBuilder.addTopicFilter(topic);
        }

        MqttUnsubscribeMessage unsubscribeMessage = unsubscribeBuilder.build();

        return sendAndAwaitAck(
                unsubscribeMessage,
                pendingKey(MqttMessageType.UNSUBACK, messageId),
                config.getUnsubscribeTimeout()
        );
    }

    @Override
    public Mono<Void> unsubscribe(Collection<String> topics) {
        if (topics == null || topics.isEmpty()) {
            return Mono.empty();
        }
        return unsubscribe(topics.toArray(new String[0]));
    }

    @Override
    public boolean isAlive() {
        Connection conn = (Connection) CONNECTION.get(this);
        return hasFlag(CONNECTED) && conn != null && conn.channel().isActive();
    }

    @Override
    public Mono<Void> onClose() {
        return closeSink.asMono();
    }

    @Override
    public Mono<Void> close() {
        return Mono.defer(() -> {
            if (flagAlreadySet(CLOSED)) {
                return Mono.empty();
            }
            cancelReconnect();
            stopHeartbeat();
            clearFlag(CONNECTED);
            clearFlag(RECONNECTING);

            Connection conn = (Connection) CONNECTION.get(this);
            if (conn != null) {
                conn.dispose();
            }
            closeSink.tryEmitEmpty();
            reconnectSink.tryEmitComplete();
            return Mono.empty();
        });
    }

    @Override
    public Mono<Void> disconnect() {
        return Mono.defer(() -> {
            if (!hasFlag(CONNECTED)) {
                return close();
            }
            return send(MqttMessage.DISCONNECT)
                    .then(close());
        });
    }

    @Override
    public Flux<Integer> onReconnect() {
        return reconnectSink.asFlux();
    }

    @Override
    public String getClientId() {
        return config.getClientId();
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        Connection conn = (Connection) CONNECTION.get(this);
        return conn != null ? (InetSocketAddress) conn.channel().remoteAddress() : null;
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        Connection conn = (Connection) CONNECTION.get(this);
        return conn != null ? (InetSocketAddress) conn.channel().localAddress() : null;
    }

    @Override
    public MqttVersion getVersion() {
        return config.getProtocolVersion();
    }

    @Override
    public MqttQoS getQos() {
        return config.getQos();
    }

    Mono<Void> send(MqttMessage message) {
        Connection conn = (Connection) CONNECTION.get(this);
        return conn.outbound()
                   .sendObject(Mono.just(message))
                   .then();
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

    /**
     * 启动心跳定时器
     * 如果 keepAlive > 0，定期发送 PINGREQ 消息
     */
    private void startHeartbeat() {
        stopHeartbeat();

        if (config.getKeepAlive() <= 0) {
            return;
        }

        // 心跳间隔设置为 keepAlive 的 75%，留有余地
        long intervalSeconds = (long) (config.getKeepAlive() * 0.75);
        if (intervalSeconds < 1) {
            intervalSeconds = 1;
        }

        long finalIntervalSeconds = intervalSeconds;
        log.log(Level.FINE, () -> "Starting heartbeat for client " + config.getClientId() + " with interval " + finalIntervalSeconds + "s");

        Connection conn = (Connection) CONNECTION.get(this);
        Disposable timer = asDisposable(
                conn.channel()
                    .eventLoop()
                    .scheduleAtFixedRate(() -> runManagedTask(
                                    sendPing(),
                                    null,
                                    error -> log.log(Level.WARNING, error, () -> "Heartbeat error for client " + config.getClientId())
                            ),
                            intervalSeconds,
                            intervalSeconds,
                            TimeUnit.SECONDS
                    )
        );
        HEARTBEAT_TIMER.setRelease(this, timer);
    }

    /**
     * 停止心跳定时器
     */
    private void stopHeartbeat() {
        Disposable timer = (Disposable) HEARTBEAT_TIMER.getAndSet(this, null);
        if (timer != null && !timer.isDisposed()) {
            timer.dispose();
            log.log(Level.FINE, () -> "Stopped heartbeat for client " + config.getClientId());
        }
    }

    /**
     * 发送 PINGREQ 消息
     */
    private Mono<Void> sendPing() {
        if (!hasFlag(CONNECTED)) {
            return Mono.empty();
        }
        return send(MqttMessage.PINGREQ)
                .doOnSuccess(v -> log.log(Level.FINEST, () -> "Sending PINGREQ for client " + config.getClientId()))
                .doOnError(error -> log.log(Level.WARNING, error, () -> "Failed to send PINGREQ for client " + config.getClientId()));
    }

    private boolean hasFlag(int flag) {
        return ((int) STATE.getVolatile(this) & flag) != 0;
    }

    private void setConnectedFlag() {
        int prev, next;
        do {
            prev = (int) STATE.getVolatile(this);
            next = prev | CONNECTED;
        } while (!STATE.compareAndSet(this, prev, next));
    }

    /**
     * 尝试设置标志位，如果已存在则返回true（设置失败）
     */
    private boolean flagAlreadySet(int flag) {
        int prev;
        do {
            prev = (int) STATE.getVolatile(this);
            if ((prev & flag) != 0) {
                return true;
            }
        } while (!STATE.compareAndSet(this, prev, prev | flag));
        return false;
    }

    private boolean clearFlag(int flag) {
        int prev;
        do {
            prev = (int) STATE.getVolatile(this);
            if ((prev & flag) == 0) {
                return false;
            }
        } while (!STATE.compareAndSet(this, prev, prev & ~flag));
        return true;
    }

    /**
     * 增加重连次数并返回新值
     */
    private int incrementReconnectAttempt() {
        return ((int) STATE.getAndAdd(this, ATTEMPT_INCREMENT) + ATTEMPT_INCREMENT) >>> 3;
    }

    private Sinks.Empty<Void> registerPendingAck(int key) {
        Sinks.Empty<Void> sink = Sinks.empty();
        pendingAcks.put(key, sink);
        return sink;
    }

    private Mono<Void> awaitPendingAck(int key) {
        return Mono.defer(() -> {
            Sinks.Empty<Void> sink = pendingAcks.get(key);
            return sink != null ? sink.asMono() : Mono.empty();
        });
    }

    private Mono<Void> sendAndAwaitAck(MqttMessage message, int ackKey, Duration timeout) {
        registerPendingAck(ackKey);
        return send(message)
                .then(awaitPendingAck(ackKey))
                .timeout(timeout)
                .doOnError(error -> pendingAcks.remove(ackKey));
    }

    /**
     * 重置重连次数为0,保留状态标志
     */
    private void resetReconnectAttempt() {
        int prev, next;
        do {
            prev = (int) STATE.getVolatile(this);
            next = prev & FLAGS_MASK;
        } while (!STATE.compareAndSet(this, prev, next));
    }

    private void cancelReconnect() {
        Disposable reconnect = (Disposable) RECONNECT_TASK.getAndSet(this, null);
        if (reconnect != null && !reconnect.isDisposed()) {
            reconnect.dispose();
        }
    }

    void runManagedTask(Mono<Void> task, Runnable onComplete, Consumer<Throwable> onError) {
        ReactiveTaskSupport.start(task, onComplete, onError);
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

    @SuppressWarnings("unchecked")
    private Sinks.One<MqttConnAckMessage> currentConnAckSink() {
        return (Sinks.One<MqttConnAckMessage>) CONN_ACK_SINK.getAcquire(this);
    }

    private void setConnAckSink(Sinks.One<MqttConnAckMessage> sink) {
        CONN_ACK_SINK.setRelease(this, sink);
    }

    private void onReconnectComplete(int attempt, Disposable task) {
        clearReconnectTask(task);
        clearFlag(RECONNECTING);
        if (hasFlag(CLOSED)) {
            return;
        }
        log.log(Level.INFO, () -> "Reconnect successful for client " + config.getClientId());
        if (reconnectSink.currentSubscriberCount() > 0) {
            reconnectSink.tryEmitNext(attempt);
        }
    }

    private void onReconnectError(Disposable task, Throwable error) {
        clearReconnectTask(task);
        clearFlag(RECONNECTING);
        if (hasFlag(CLOSED)) {
            return;
        }
        log.log(Level.WARNING, () -> "Reconnect failed for client " + config.getClientId() + ": " + error.getMessage());
        attemptReconnect();
    }

    private void clearReconnectTask(Disposable task) {
        RECONNECT_TASK.compareAndSet(this, task, null);
    }

    public static class MqttConnectionException extends RuntimeException {
        public MqttConnectionException(String message) {
            super(message);
        }
    }
}
