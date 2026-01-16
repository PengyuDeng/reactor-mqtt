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
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.jetlinks.reactor.mqtt.MqttConstants.Message.DISCONNECT_MESSAGE;
import static org.jetlinks.reactor.mqtt.MqttConstants.Message.Header.PUBCOMP_HEADER;
import static org.jetlinks.reactor.mqtt.MqttConstants.Message.Header.PUBREL_HEADER;
import static org.jetlinks.reactor.mqtt.MqttConstants.Message.PING_MESSAGE;

/**
 * MQTT 客户端连接实现
 *
 * @author PengyuDeng
 */
public class DefaultClientConnection implements ClientConnection {

    private static final Logger log = Logger.getLogger(DefaultClientConnection.class.getName());


    /**
     * 连接配置,包含所有 MQTT 连接参数
     */
    private final ConnectionConfig config;

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

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            STATE = lookup.findVarHandle(DefaultClientConnection.class, "state", int.class);
            MESSAGE_ID_GENERATOR = lookup.findVarHandle(DefaultClientConnection.class, "messageIdGenerator", int.class);
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
    private volatile Sinks.One<MqttConnAckMessage> connAckSink = Sinks.one();

    /**
     * MQTT 消息 ID 生成器,使用 VarHandle 保证原子性和可见性
     * 范围 1-65535,循环使用
     */
    @SuppressWarnings("unused")
    private volatile int messageIdGenerator = 0;

    /**
     * 统一的 pending 消息映射: key = (MqttMessageType.ordinal << 16) | messageId
     */
    private final Map<Integer, Sinks.Empty<Void>> pendingAcks = new ConcurrentHashMap<>();

    /**
     * 订阅管理器,统一管理订阅和处理器
     */
    private final SubscriptionManager subscriptionManager;

    /**
     * 心跳定时器
     */
    private volatile Disposable heartbeatTimer;

    public DefaultClientConnection(Connection connection,
                                   String clientId,
                                   String username,
                                   byte[] password,
                                   int keepAlive,
                                   boolean cleanSession,
                                   byte protocolVersion,
                                   MqttWillMessage willMessage,
                                   Consumer<ClientReceivedPublish> publishingHandler,
                                   boolean autoAck,
                                   MqttQoS qos,
                                   ReconnectStrategy reconnectStrategy,
                                   boolean autoResubscribe,
                                   Supplier<TcpClient> tcpClientSupplier,
                                   Duration subscribeTimeout,
                                   Duration unsubscribeTimeout,
                                   Duration publishTimeout,
                                   SubscriptionManager subscriptionManager) {

        this.connection = connection;
        this.config = new ConnectionConfig(clientId, username, password, keepAlive, cleanSession, protocolVersion,
                                           willMessage, publishingHandler, autoAck, qos,
                                           reconnectStrategy, autoResubscribe,
                                           subscribeTimeout, unsubscribeTimeout, publishTimeout);
        this.tcpClientSupplier = tcpClientSupplier;
        this.subscriptionManager = subscriptionManager != null
                ? subscriptionManager
                : SubscriptionManager.create();
    }

    private static int pendingKey(MqttMessageType type, int messageId) {
        return (type.value() << 16) | messageId;
    }

    /**
     * 初始化连接（发送 CONNECT，等待 CONNACK）
     */
    Mono<ClientConnection> initialize() {
        ensureMqttCodec();
        setupConnectionHandlers();
        return sendConnect()
                .doOnSuccess(v -> startHeartbeat())
                .then(Mono.just(this));
    }

    private void ensureMqttCodec() {
        try {
            if (connection.channel().pipeline().get("mqttEncoder") == null) {
                connection.addHandlerFirst("mqttEncoder", MqttEncoder.INSTANCE);
            }
            if (connection.channel().pipeline().get("mqttDecoder") == null) {
                connection.addHandlerFirst("mqttDecoder", new MqttDecoder(8096));
            }
        } catch (Exception e) {
            if (log.isLoggable(Level.WARNING)) {
                log.log(Level.WARNING, "Failed to add MQTT codec: " + e.getMessage(), e);
            }
        }
    }

    private void setupConnectionHandlers() {
        connection.inbound()
                  .receiveObject()
                  .cast(MqttMessage.class)
                  .flatMap(this::handleMessage)
                  .subscribe(
                          v -> {
                          },
                          this::handleError,
                          this::handleComplete
                  );

        connection.onDispose(() -> {
            if (!hasFlag(CLOSED)) {
                handleDisconnect();
            }
        });
    }

    private Mono<Void> sendConnect() {
        MqttMessageBuilders.ConnectBuilder builder = MqttMessageBuilders.connect()
                                                                        .clientId(config.clientId)
                                                                        .keepAlive(config.keepAlive)
                                                                        .cleanSession(config.cleanSession());

        if (config.protocolVersion == 5) {
            builder.protocolVersion(MqttVersion.MQTT_5);
        }

        if (config.username != null) {
            builder.username(config.username);
            if (config.password != null) {
                builder.password(config.password);
            }
        }

        if (config.willMessage != null && config.willMessage.hasWill()) {
            builder.willTopic(config.willMessage.topic())
                   .willMessage(config.willMessage.payloadBytes())
                   .willQoS(config.willMessage.qos())
                   .willRetain(config.willMessage.retain());
        }

        MqttConnectMessage connectMessage = builder.build();

        return send(connectMessage)
                .then(connAckSink.asMono()
                                 .timeout(Duration.ofSeconds(10))
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
        Sinks.EmitResult result = connAckSink.tryEmitValue(msg);
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

        DefaultClientReceivedPublish publishing = new DefaultClientReceivedPublish(msg, this);

        // 委托给 SubscriptionManager 处理订阅匹配
        Mono<Void> handlerMono = subscriptionManager.handleMessage(publishing);

        // 全局处理器
        if (config.publishingHandler != null) {
            handlerMono = handlerMono.then(Mono.fromRunnable(() -> config.publishingHandler.accept(publishing)));
        }

        // 自动确认
        if (config.autoAck() && msg.fixedHeader().qosLevel() != MqttQoS.AT_MOST_ONCE) {
            handlerMono = handlerMono.then(publishing.acknowledge());
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
        if (log.isLoggable(Level.WARNING)) {
            log.log(Level.WARNING, "Connection error for client " + config.clientId + ": " + error.getMessage(), error);
        }
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
        if (flagAlreadySet(RECONNECTING)) {
            return;
        }

        int attempt = incrementReconnectAttempt();

        connAckSink = Sinks.one();

        config.reconnectStrategy.nextDelay(attempt, null)
                                .flatMap(delay -> Mono.delay(delay)
                                                      .then(tcpClientSupplier.get().connect())
                                                      .flatMap(conn -> {
                                                          this.connection = conn;
                                                          ensureMqttCodec();
                                                          setupConnectionHandlers();
                                                          return sendConnect();
                                                      })
                                                      .then(resubscribeIfNeeded())
                                )
                                .subscribe(
                                        v -> clearFlag(RECONNECTING),
                                        error -> {
                                            clearFlag(RECONNECTING);
                                            log.log(Level.WARNING, () -> "Reconnect failed for client " + config.clientId + ": " + error.getMessage());
                                            attemptReconnect();
                                        },
                                        () -> {
                                            clearFlag(RECONNECTING);
                                            log.log(Level.WARNING, () -> "Max reconnect attempts reached for client " + config.clientId + ", closing connection");
                                            close().subscribe();
                                        }
                                );
    }

    private Mono<Void> resubscribeIfNeeded() {
        if (!config.autoResubscribe()) {
            return Mono.empty();
        }

        Iterable<SubscriptionManager.SubscriptionInfo> subscriptions = subscriptionManager.getSubscriptions();

        // 检查是否有订阅
        boolean hasSubscriptions = subscriptions.iterator().hasNext();
        if (!hasSubscriptions) {
            return Mono.empty();
        }

        return Flux.fromIterable(subscriptions)
                   .flatMap(sub -> doSubscribe(sub.topic(), sub.qos()))
                   .then();
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
                Sinks.Empty<Void> sink = Sinks.empty();
                pendingAcks.put(pendingKey(MqttMessageType.PUBACK, messageId), sink);
                return send(finalMessage)
                        .then(sink.asMono())
                        .timeout(config.publishTimeout)
                        .doOnError(e -> pendingAcks.remove(pendingKey(MqttMessageType.PUBACK, messageId)));
            } else {
                // QoS 2
                Sinks.Empty<Void> recSink = Sinks.empty();
                pendingAcks.put(pendingKey(MqttMessageType.PUBREC, messageId), recSink);
                return send(finalMessage)
                        .then(recSink.asMono())
                        .then(Mono.defer(() -> {
                            Sinks.Empty<Void> compSink = pendingAcks.get(pendingKey(MqttMessageType.PUBCOMP, messageId));
                            return compSink != null ? compSink.asMono() : Mono.empty();
                        }))
                        .timeout(config.publishTimeout)
                        .doOnError(e -> {
                            pendingAcks.remove(pendingKey(MqttMessageType.PUBREC, messageId));
                            pendingAcks.remove(pendingKey(MqttMessageType.PUBCOMP, messageId));
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

        Sinks.Empty<Void> sink = Sinks.empty();
        pendingAcks.put(pendingKey(MqttMessageType.SUBACK, messageId), sink);

        return send(subscribeMessage)
                .then(sink.asMono())
                .timeout(config.subscribeTimeout)
                .doOnError(e -> pendingAcks.remove(pendingKey(MqttMessageType.SUBACK, messageId)));
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

        Sinks.Empty<Void> sink = Sinks.empty();
        pendingAcks.put(pendingKey(MqttMessageType.UNSUBACK, messageId), sink);

        return send(unsubscribeMessage)
                .then(sink.asMono())
                .timeout(config.unsubscribeTimeout)
                .doOnError(e -> pendingAcks.remove(pendingKey(MqttMessageType.UNSUBACK, messageId)));
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
        return hasFlag(CONNECTED) && connection != null && connection.channel().isActive();
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
            stopHeartbeat();
            clearFlag(CONNECTED);

            if (connection != null) {
                connection.dispose();
            }
            closeSink.tryEmitEmpty();
            return Mono.empty();
        });
    }

    @Override
    public Mono<Void> disconnect() {
        return Mono.defer(() -> {
            if (!hasFlag(CONNECTED)) {
                return close();
            }
            return send(DISCONNECT_MESSAGE)
                    .then(close());
        });
    }

    @Override
    public String getClientId() {
        return config.clientId;
    }

    @Override
    public MqttQoS getQos() {
        return config.qos;
    }

    Mono<Void> send(MqttMessage message) {
        return connection.outbound()
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

        if (config.keepAlive <= 0) {
            return;
        }

        // 心跳间隔设置为 keepAlive 的 75%，留有余地
        long intervalSeconds = (long) (config.keepAlive * 0.75);
        if (intervalSeconds < 1) {
            intervalSeconds = 1;
        }

        if (log.isLoggable(Level.FINE)) {
            log.fine("Starting heartbeat for client " + config.clientId + " with interval " + intervalSeconds + "s");
        }

        heartbeatTimer = Flux.interval(Duration.ofSeconds(intervalSeconds), Schedulers.parallel())
                             .flatMap(tick -> sendPing())
                             .subscribe(
                                     v -> {
                                     },
                                     error -> {
                                         if (log.isLoggable(Level.WARNING)) {
                                             log.log(Level.WARNING, "Heartbeat error for client " + config.clientId, error);
                                         }
                                     }
                             );
    }

    /**
     * 停止心跳定时器
     */
    private void stopHeartbeat() {
        Disposable timer = heartbeatTimer;
        if (timer != null && !timer.isDisposed()) {
            timer.dispose();
            heartbeatTimer = null;
            if (log.isLoggable(Level.FINE)) {
                log.fine("Stopped heartbeat for client " + config.clientId);
            }
        }
    }

    /**
     * 发送 PINGREQ 消息
     */
    private Mono<Void> sendPing() {
        if (!hasFlag(CONNECTED)) {
            return Mono.empty();
        }
        return send(PING_MESSAGE)
                .doOnSuccess(v -> {
                    if (log.isLoggable(Level.FINEST)) {
                        log.finest("Sending PINGREQ for client " + config.clientId);
                    }
                })
                .doOnError(error -> {
                    if (log.isLoggable(Level.WARNING)) {
                        log.log(Level.WARNING, "Failed to send PINGREQ for client " + config.clientId, error);
                    }
                });
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
                return true;  // 已存在
            }
        } while (!STATE.compareAndSet(this, prev, prev | flag));
        return false;  // 设置成功
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

    /**
     * 连接配置(不可变)
     */
    private static class ConnectionConfig {
        private static final byte FLAG_CLEAN_SESSION = 1;
        private static final byte FLAG_AUTO_ACK = 2;
        private static final byte FLAG_AUTO_RESUBSCRIBE = 4;

        final String clientId;
        final String username;
        final byte[] password;
        final int keepAlive;
        final byte protocolVersion;
        final byte flags;
        final MqttWillMessage willMessage;
        final MqttQoS qos;
        final Consumer<ClientReceivedPublish> publishingHandler;
        final ReconnectStrategy reconnectStrategy;
        final Duration subscribeTimeout;
        final Duration unsubscribeTimeout;
        final Duration publishTimeout;

        ConnectionConfig(String clientId, String username, byte[] password,
                         int keepAlive, boolean cleanSession, byte protocolVersion,
                         MqttWillMessage willMessage,
                         Consumer<ClientReceivedPublish> publishingHandler,
                         boolean autoAck, MqttQoS qos,
                         ReconnectStrategy reconnectStrategy, boolean autoResubscribe,
                         Duration subscribeTimeout, Duration unsubscribeTimeout, Duration publishTimeout) {
            this.clientId = clientId;
            this.username = username;
            this.password = password;
            this.keepAlive = keepAlive;
            this.protocolVersion = protocolVersion;
            this.flags = (byte) ((cleanSession ? FLAG_CLEAN_SESSION : 0)
                    | (autoAck ? FLAG_AUTO_ACK : 0)
                    | (autoResubscribe ? FLAG_AUTO_RESUBSCRIBE : 0));
            this.willMessage = willMessage;
            this.qos = qos;
            this.publishingHandler = publishingHandler;
            this.reconnectStrategy = reconnectStrategy;
            this.subscribeTimeout = subscribeTimeout;
            this.unsubscribeTimeout = unsubscribeTimeout;
            this.publishTimeout = publishTimeout;
        }

        boolean cleanSession() {
            return (flags & FLAG_CLEAN_SESSION) != 0;
        }

        boolean autoAck() {
            return (flags & FLAG_AUTO_ACK) != 0;
        }

        boolean autoResubscribe() {
            return (flags & FLAG_AUTO_RESUBSCRIBE) != 0;
        }
    }

    public static class MqttConnectionException extends RuntimeException {
        public MqttConnectionException(String message) {
            super(message);
        }
    }
}
