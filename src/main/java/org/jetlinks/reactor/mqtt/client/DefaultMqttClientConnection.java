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
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MQTT 客户端连接实现
 *
 * @author PengyuDeng
 */
public class DefaultMqttClientConnection implements MqttClientConnection {

    private static final Logger log = Logger.getLogger(DefaultMqttClientConnection.class.getName());

    /**
     * 连接配置,包含所有 MQTT 连接参数
     */
    private final ConnectionConfig config;

    /**
     * TCP 客户端提供者,用于创建新连接
     */
    private final Supplier<TcpClient> tcpClientSupplier;

    /**
     * Netty TCP 连接,使用 volatile 保证重连时的可见性
     */
    private volatile Connection connection;

    /**
     * 状态字段: bit0-2=状态标志, bit3-31=重连次数
     * 标志位: bit0=connected, bit1=closed, bit2=reconnecting
     * AtomicInteger state 布局:</br>
     * ┌─────────────────────────────────┬────┬────┬────┐
     * │  重连次数 (29 bits)              │ R  │ C  │ N  │
     * │  bit 3-31                       │bit2│bit1│bit0│
     * └─────────────────────────────────┴────┴────┴────┘
     * N = CONNECTED (1)
     * C = CLOSED (2)
     * R = RECONNECTING (4)
     */
    private final AtomicInteger state = new AtomicInteger(0);

    // 状态标志位 (bit 0-2)
    private static final int CONNECTED = 1;
    private static final int CLOSED = 2;
    private static final int RECONNECTING = 4;
    private static final int FLAGS_MASK = 0x7;          // 低3位
    private static final int ATTEMPT_INCREMENT = 0x8;   // 重连次数从 bit3 开始

    /**
     * 关闭完成信号,用于 onClose() 方法
     */
    private final Sinks.Empty<Void> closeSink = Sinks.empty();

    /**
     * CONNACK 消息接收器,每次重连时重置,使用 volatile 保证可见性
     */
    private volatile Sinks.One<MqttConnAckMessage> connAckSink = Sinks.one();

    /**
     * MQTT 消息 ID 生成器,范围 1-65535,循环使用
     */
    private final AtomicInteger messageIdGenerator = new AtomicInteger(0);

    /**
     * 统一的 pending 消息映射: key = (MqttMessageType.ordinal << 16) | messageId
     */
    private final Map<Integer, Sinks.Empty<Void>> pendingAcks = new ConcurrentHashMap<>();

    /**
     * 订阅处理器映射,key 为主题,value 为处理器(同时用于重连后自动重新订阅)
     */
    private final Map<String, SubscriptionHandler> subscriptionHandlers = new ConcurrentHashMap<>();

    /**
     * 接收到的消息流,支持多个订阅者
     */
    private final Sinks.Many<MqttClientPublishing> messageSink = Sinks.many().multicast().onBackpressureBuffer();

    public DefaultMqttClientConnection(Connection connection,
                                       String clientId,
                                       String username,
                                       byte[] password,
                                       short keepAliveSeconds,
                                       boolean cleanSession,
                                       byte protocolVersion,
                                       String willTopic,
                                       ByteBuf willPayload,
                                       MqttQoS willQos,
                                       boolean willRetain,
                                       Function<MqttClientPublishing, Mono<Void>> publishingHandler,
                                       boolean autoAck,
                                       MqttQoS qos,
                                       ReconnectStrategy reconnectStrategy,
                                       boolean autoResubscribe,
                                       Supplier<TcpClient> tcpClientSupplier) {

        this.connection = connection;
        this.config = new ConnectionConfig(clientId, username, password, keepAliveSeconds, cleanSession, protocolVersion,
                                           willTopic, willPayload, willQos, willRetain, publishingHandler, autoAck, qos,
                                           reconnectStrategy, autoResubscribe);
        this.tcpClientSupplier = tcpClientSupplier;
    }

    private static int pendingKey(MqttMessageType type, int messageId) {
        return (type.value() << 16) | messageId;
    }

    /**
     * 初始化连接（发送 CONNECT，等待 CONNACK）
     */
    Mono<MqttClientConnection> initialize() {
        ensureMqttCodec();
        setupConnectionHandlers();
        return sendConnect()
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
            log.log(Level.WARNING, "Failed to add MQTT codec: " + e.getMessage(), e);
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
                                                                        .keepAlive(config.keepAliveSeconds)
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

        if (config.willTopic != null) {
            byte[] willPayloadBytes = null;
            if (config.willPayload != null && config.willPayload.readableBytes() > 0) {
                willPayloadBytes = new byte[config.willPayload.readableBytes()];
                config.willPayload.getBytes(config.willPayload.readerIndex(), willPayloadBytes);
            }
            builder.willTopic(config.willTopic)
                   .willMessage(willPayloadBytes)
                   .willQoS(config.willQos)
                   .willRetain(config.willRetain());
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
            log.log(Level.WARNING, "Failed to emit CONNACK: " + result + ", this should not happen after reconnect fix");
        }
        return Mono.empty();
    }

    private Mono<Void> handlePublish(MqttPublishMessage msg) {
        try {
            ReferenceCountUtil.retain(msg);
        } catch (Exception e) {
            return Mono.empty();
        }

        DefaultMqttClientPublishing publishing = new DefaultMqttClientPublishing(msg, this::send);

        // 发送到消息流
        messageSink.tryEmitNext(publishing);

        // 匹配订阅处理器
        String topic = msg.variableHeader().topicName();
        Mono<Void> handlerMono = Mono.empty();

        for (Map.Entry<String, SubscriptionHandler> entry : subscriptionHandlers.entrySet()) {
            if (topicMatches(entry.getKey(), topic)) {
                SubscriptionHandler handler = entry.getValue();
                if (handler.handler != null) {
                    handlerMono = handlerMono.then(handler.handler.apply(publishing));
                }
            }
        }

        // 全局处理器
        if (config.publishingHandler != null) {
            handlerMono = handlerMono.then(config.publishingHandler.apply(publishing));
        }

        // 自动确认
        if (config.autoAck() && msg.fixedHeader().qosLevel() != MqttQoS.AT_MOST_ONCE) {
            handlerMono = handlerMono.then(publishing.acknowledge());
        }

        return handlerMono.doFinally(signal -> publishing.release());
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
                new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                MqttMessageIdVariableHeader.from(messageId)
        );

        Sinks.Empty<Void> compSink = Sinks.empty();
        pendingAcks.put(pendingKey(MqttMessageType.PUBCOMP, messageId), compSink);

        return send(pubRel);
    }

    private Mono<Void> handlePubRel(MqttMessage msg) {
        int messageId = ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();

        MqttMessage pubComp = new MqttMessage(
                new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_MOST_ONCE, false, 0),
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
        log.log(Level.WARNING, "Connection error: " + error.getMessage(), error);
        handleDisconnect();
    }

    private void handleComplete() {
        handleDisconnect();
    }

    private void handleDisconnect() {
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
                                        v -> {
                                            clearFlag(RECONNECTING);
                                            log.info("Reconnected successfully");
                                        },
                                        error -> {
                                            clearFlag(RECONNECTING);
                                            log.log(Level.WARNING, "Reconnect failed: " + error.getMessage());
                                            attemptReconnect();
                                        }
                                );
    }

    private Mono<Void> resubscribeIfNeeded() {
        if (!config.autoResubscribe() || subscriptionHandlers.isEmpty()) {
            return Mono.empty();
        }

        return Flux.fromIterable(subscriptionHandlers.values())
                   .flatMap(sub -> doSubscribe(sub.topic, sub.qos))
                   .then();
    }

    @Override
    public Mono<Void> publish(String topic, ByteBuf payload, MqttQoS qos, boolean retain) {
        return Mono.defer(() -> {
            if (!hasFlag(CONNECTED)) {
                return Mono.error(new IllegalStateException("Not connected"));
            }

            int messageId = qos == MqttQoS.AT_MOST_ONCE ? 0 : nextMessageId();

            MqttPublishMessage publishMessage = MqttMessageBuilders.publish()
                                                                   .topicName(topic)
                                                                   .payload(payload != null ? payload : Unpooled.EMPTY_BUFFER)
                                                                   .qos(qos)
                                                                   .retained(retain)
                                                                   .messageId(messageId)
                                                                   .build();

            if (qos == MqttQoS.AT_MOST_ONCE) {
                return send(publishMessage);
            } else if (qos == MqttQoS.AT_LEAST_ONCE) {
                Sinks.Empty<Void> sink = Sinks.empty();
                pendingAcks.put(pendingKey(MqttMessageType.PUBACK, messageId), sink);
                return send(publishMessage)
                        .then(sink.asMono())
                        .timeout(Duration.ofSeconds(30))
                        .doOnError(e -> pendingAcks.remove(pendingKey(MqttMessageType.PUBACK, messageId)));
            } else {
                // QoS 2
                Sinks.Empty<Void> recSink = Sinks.empty();
                pendingAcks.put(pendingKey(MqttMessageType.PUBREC, messageId), recSink);
                return send(publishMessage)
                        .then(recSink.asMono())
                        .then(Mono.defer(() -> {
                            Sinks.Empty<Void> compSink = pendingAcks.get(pendingKey(MqttMessageType.PUBCOMP, messageId));
                            return compSink != null ? compSink.asMono() : Mono.empty();
                        }))
                        .timeout(Duration.ofSeconds(30))
                        .doOnError(e -> {
                            pendingAcks.remove(pendingKey(MqttMessageType.PUBREC, messageId));
                            pendingAcks.remove(pendingKey(MqttMessageType.PUBCOMP, messageId));
                        });
            }
        });
    }

    @Override
    public Disposable subscribe(String topic, MqttQoS qos, Function<MqttClientPublishing, Mono<Void>> handler) {
        SubscriptionHandler subHandler = new SubscriptionHandler(topic, qos, handler);
        subscriptionHandlers.put(topic, subHandler);

        Disposable.Composite composite = Disposables.composite();

        composite.add(doSubscribe(topic, qos).subscribe());
        composite.add(() -> {
            subscriptionHandlers.remove(topic);
            unsubscribe(topic).subscribe();
        });

        return composite;
    }

    private Mono<Void> doSubscribe(String topic, MqttQoS qos) {
        int messageId = nextMessageId();

        MqttSubscribeMessage subscribeMessage = MqttMessageBuilders.subscribe()
                                                                   .messageId(messageId)
                                                                   .addSubscription(qos, topic)
                                                                   .build();

        Sinks.Empty<Void> sink = Sinks.empty();
        pendingAcks.put(pendingKey(MqttMessageType.SUBACK, messageId), sink);

        return send(subscribeMessage)
                .then(sink.asMono())
                .timeout(Duration.ofSeconds(10))
                .doOnError(e -> pendingAcks.remove(pendingKey(MqttMessageType.SUBACK, messageId)));
    }

    @Override
    public Mono<Void> subscribe(String... topics) {
        if (topics == null || topics.length == 0) {
            return Mono.empty();
        }

        int messageId = nextMessageId();

        MqttMessageBuilders.SubscribeBuilder subscribeBuilder = MqttMessageBuilders.subscribe()
                                                                                   .messageId(messageId);

        for (String topic : topics) {
            subscribeBuilder.addSubscription(MqttQoS.AT_MOST_ONCE, topic);
            // 记录订阅但不带处理器(仅用于重连)
            subscriptionHandlers.putIfAbsent(topic, new SubscriptionHandler(topic, MqttQoS.AT_MOST_ONCE, null));
        }

        MqttSubscribeMessage subscribeMessage = subscribeBuilder.build();

        Sinks.Empty<Void> sink = Sinks.empty();
        pendingAcks.put(pendingKey(MqttMessageType.SUBACK, messageId), sink);

        return send(subscribeMessage)
                .then(sink.asMono())
                .timeout(Duration.ofSeconds(10))
                .doOnError(e -> pendingAcks.remove(pendingKey(MqttMessageType.SUBACK, messageId)));
    }

    @Override
    public Mono<Void> subscribe(Collection<String> topics) {
        if (topics == null || topics.isEmpty()) {
            return Mono.empty();
        }
        return subscribe(topics.toArray(new String[0]));
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
            subscriptionHandlers.remove(topic);
        }

        MqttUnsubscribeMessage unsubscribeMessage = unsubscribeBuilder.build();

        Sinks.Empty<Void> sink = Sinks.empty();
        pendingAcks.put(pendingKey(MqttMessageType.UNSUBACK, messageId), sink);

        return send(unsubscribeMessage)
                .then(sink.asMono())
                .timeout(Duration.ofSeconds(10))
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
    public Flux<MqttClientPublishing> receive() {
        return messageSink.asFlux();
    }

    @Override
    public boolean isConnected() {
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
            clearFlag(CONNECTED);
            messageSink.tryEmitComplete();

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

            MqttMessage disconnectMessage = new MqttMessage(
                    new MqttFixedHeader(MqttMessageType.DISCONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0)
            );

            return send(disconnectMessage)
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

    private Mono<Void> send(MqttMessage message) {
        return connection.outbound()
                         .sendObject(Mono.just(message))
                         .then();
    }

    /**
     * 生成下一个 MQTT 消息 ID,范围 1-65535
     *
     * @return 消息 ID (1-65535)
     */
    private int nextMessageId() {
        int id;
        do {
            id = messageIdGenerator.incrementAndGet() & 0xFFFF;
        } while (id == 0);
        return id;
    }

    private boolean topicMatches(String filter, String topic) {
        if (filter.equals(topic)) {
            return true;
        }

        String[] filterParts = filter.split("/");
        String[] topicParts = topic.split("/");

        for (int i = 0; i < filterParts.length; i++) {
            String filterPart = filterParts[i];

            if (filterPart.equals("#")) {
                return true;
            }

            if (i >= topicParts.length) {
                return false;
            }

            if (!filterPart.equals("+") && !filterPart.equals(topicParts[i])) {
                return false;
            }
        }

        return filterParts.length == topicParts.length;
    }

    private boolean hasFlag(int flag) {
        return (state.get() & flag) != 0;
    }

    private void setConnectedFlag() {
        state.updateAndGet(v -> v | DefaultMqttClientConnection.CONNECTED);
    }

    /**
     * 尝试设置标志位，如果已存在则返回true（设置失败）
     */
    private boolean flagAlreadySet(int flag) {
        int prev;
        do {
            prev = state.get();
            if ((prev & flag) != 0) {
                return true;  // 已存在
            }
        } while (!state.compareAndSet(prev, prev | flag));
        return false;  // 设置成功
    }

    private boolean clearFlag(int flag) {
        int prev;
        do {
            prev = state.get();
            if ((prev & flag) == 0) {
                return false;
            }
        } while (!state.compareAndSet(prev, prev & ~flag));
        return true;
    }

    /**
     * 增加重连次数并返回新值
     */
    private int incrementReconnectAttempt() {
        return (state.addAndGet(ATTEMPT_INCREMENT) >>> 3);
    }

    /**
     * 重置重连次数为0,保留状态标志
     */
    private void resetReconnectAttempt() {
        state.updateAndGet(v -> v & FLAGS_MASK);
    }

    /**
     * 订阅处理器,同时用于重连后自动重新订阅
     */
    private static class SubscriptionHandler {
        final String topic;
        final MqttQoS qos;
        final Function<MqttClientPublishing, Mono<Void>> handler;

        SubscriptionHandler(String topic, MqttQoS qos, Function<MqttClientPublishing, Mono<Void>> handler) {
            this.topic = topic;
            this.qos = qos;
            this.handler = handler;
        }
    }

    /**
     * 连接配置(不可变)
     */
    private static class ConnectionConfig {
        // 布尔标志位
        private static final byte FLAG_CLEAN_SESSION = 1;
        private static final byte FLAG_WILL_RETAIN = 2;
        private static final byte FLAG_AUTO_ACK = 4;
        private static final byte FLAG_AUTO_RESUBSCRIBE = 8;

        final String clientId;
        final String username;
        final byte[] password;
        final short keepAliveSeconds;          // 原 int, 改为 short (最大 65535 秒足够)
        final byte protocolVersion;            // 原 int, 只有 3/5 两个值
        final byte flags;                      // 4 个布尔值打包
        final String willTopic;
        final ByteBuf willPayload;
        final MqttQoS willQos;
        final MqttQoS qos;
        final Function<MqttClientPublishing, Mono<Void>> publishingHandler;
        final ReconnectStrategy reconnectStrategy;

        ConnectionConfig(String clientId, String username, byte[] password,
                         short keepAliveSeconds, boolean cleanSession, byte protocolVersion,
                         String willTopic, ByteBuf willPayload, MqttQoS willQos, boolean willRetain,
                         Function<MqttClientPublishing, Mono<Void>> publishingHandler,
                         boolean autoAck, MqttQoS qos,
                         ReconnectStrategy reconnectStrategy, boolean autoResubscribe) {
            this.clientId = clientId;
            this.username = username;
            this.password = password;
            this.keepAliveSeconds = keepAliveSeconds;
            this.protocolVersion = protocolVersion;
            this.flags = (byte) ((cleanSession ? FLAG_CLEAN_SESSION : 0)
                    | (willRetain ? FLAG_WILL_RETAIN : 0)
                    | (autoAck ? FLAG_AUTO_ACK : 0)
                    | (autoResubscribe ? FLAG_AUTO_RESUBSCRIBE : 0));
            this.willTopic = willTopic;
            this.willPayload = willPayload;
            this.willQos = willQos;
            this.qos = qos;
            this.publishingHandler = publishingHandler;
            this.reconnectStrategy = reconnectStrategy;
        }

        boolean cleanSession() {
            return (flags & FLAG_CLEAN_SESSION) != 0;
        }

        boolean willRetain() {
            return (flags & FLAG_WILL_RETAIN) != 0;
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
