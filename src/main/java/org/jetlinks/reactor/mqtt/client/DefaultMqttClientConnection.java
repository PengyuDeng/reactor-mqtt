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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
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

    // ==================== 连接配置 ====================

    /**
     * Netty TCP 连接,使用 volatile 保证重连时的可见性
     */
    private volatile Connection connection;

    /**
     * MQTT 客户端 ID,用于标识客户端
     */
    private final String clientId;

    /**
     * MQTT 连接用户名,可选
     */
    private final String username;

    /**
     * MQTT 连接密码,可选
     */
    private final byte[] password;

    /**
     * MQTT Keep Alive 时间(秒),用于心跳检测
     */
    private final int keepAliveSeconds;

    /**
     * 是否使用 Clean Session,true 时服务器不保留会话状态
     */
    private final boolean cleanSession;

    /**
     * MQTT 协议版本(3 或 5)
     */
    private final int protocolVersion;

    // ==================== 遗言配置 ====================

    /**
     * 遗言消息的主题,客户端异常断开时发送
     */
    private final String willTopic;

    /**
     * 遗言消息的负载内容
     */
    private final ByteBuf willPayload;

    /**
     * 遗言消息的 QoS 级别
     */
    private final MqttQoS willQos;

    /**
     * 遗言消息是否保留
     */
    private final boolean willRetain;

    // ==================== 消息处理配置 ====================

    /**
     * 全局消息发布处理器,接收所有订阅的消息
     */
    private final Function<MqttClientPublishing, Mono<Void>> publishingHandler;

    /**
     * 是否自动确认(Acknowledge)消息,QoS > 0 时有效
     */
    private final boolean autoAck;

    /**
     * 默认发布消息的 QoS 级别
     */
    private final MqttQoS qos;

    // ==================== 重连配置 ====================

    /**
     * 重连策略,决定重连延迟时间
     */
    private final ReconnectStrategy reconnectStrategy;

    /**
     * 重连后是否自动重新订阅之前的主题
     */
    private final boolean autoResubscribe;

    /**
     * TCP 客户端提供者,用于创建新连接
     */
    private final Supplier<TcpClient> tcpClientSupplier;

    /**
     * 当前重连尝试次数,成功连接后重置为 0
     */
    private final AtomicInteger reconnectAttempt = new AtomicInteger(0);

    /**
     * 是否正在重连中,防止并发重连
     */
    private final AtomicBoolean reconnecting = new AtomicBoolean(false);

    // ==================== 连接状态 ====================

    /**
     * 是否已连接,true 表示 CONNACK 已接收且连接成功
     */
    private final AtomicBoolean connected = new AtomicBoolean(false);

    /**
     * 是否已关闭,true 表示用户主动关闭连接,不再重连
     */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * 关闭完成信号,用于 onClose() 方法
     */
    private final Sinks.Empty<Void> closeSink = Sinks.empty();

    /**
     * CONNACK 消息接收器,每次重连时重置,使用 volatile 保证可见性
     */
    private volatile Sinks.One<MqttConnAckMessage> connAckSink = Sinks.one();

    // ==================== 消息 ID 管理 ====================

    /**
     * MQTT 消息 ID 生成器,范围 1-65535,循环使用
     */
    private final AtomicInteger messageIdGenerator = new AtomicInteger(0);

    // ==================== Pending 消息(等待服务器确认) ====================

    /**
     * 等待 PUBACK 的消息(QoS 1),key 为消息 ID
     */
    private final Map<Integer, Sinks.Empty<Void>> pendingPubAck = new ConcurrentHashMap<>();

    /**
     * 等待 PUBREC 的消息(QoS 2),key 为消息 ID
     */
    private final Map<Integer, Sinks.Empty<Void>> pendingPubRec = new ConcurrentHashMap<>();

    /**
     * 等待 PUBCOMP 的消息(QoS 2),key 为消息 ID
     */
    private final Map<Integer, Sinks.Empty<Void>> pendingPubComp = new ConcurrentHashMap<>();

    /**
     * 等待 SUBACK 的订阅请求,key 为消息 ID
     */
    private final Map<Integer, Sinks.Empty<Void>> pendingSubAck = new ConcurrentHashMap<>();

    /**
     * 等待 UNSUBACK 的取消订阅请求,key 为消息 ID
     */
    private final Map<Integer, Sinks.Empty<Void>> pendingUnsubAck = new ConcurrentHashMap<>();

    // ==================== 订阅管理 ====================

    /**
     * 订阅处理器映射,key 为主题,value 为处理器
     */
    private final Map<String, SubscriptionHandler> subscriptionHandlers = new ConcurrentHashMap<>();

    /**
     * 活跃订阅列表,用于重连后自动重新订阅
     */
    private final List<SubscriptionInfo> activeSubscriptions = new CopyOnWriteArrayList<>();

    // ==================== 消息流 ====================

    /**
     * 接收到的消息流,支持多个订阅者
     */
    private final Sinks.Many<MqttClientPublishing> messageSink = Sinks.many().multicast().onBackpressureBuffer();

    public DefaultMqttClientConnection(Connection connection,
                                       String clientId,
                                       String username,
                                       byte[] password,
                                       int keepAliveSeconds,
                                       boolean cleanSession,
                                       int protocolVersion,
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
        this.clientId = clientId;
        this.username = username;
        this.password = password;
        this.keepAliveSeconds = keepAliveSeconds;
        this.cleanSession = cleanSession;
        this.protocolVersion = protocolVersion;
        this.willTopic = willTopic;
        this.willPayload = willPayload;
        this.willQos = willQos;
        this.willRetain = willRetain;
        this.publishingHandler = publishingHandler;
        this.autoAck = autoAck;
        this.qos = qos;
        this.reconnectStrategy = reconnectStrategy;
        this.autoResubscribe = autoResubscribe;
        this.tcpClientSupplier = tcpClientSupplier;
    }

    /**
     * 初始化连接（发送 CONNECT，等待 CONNACK）
     */
    Mono<MqttClientConnection> initialize() {
        // 确保 MQTT codec 已添加到 pipeline
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
            if (!closed.get()) {
                handleDisconnect();
            }
        });
    }

    private Mono<Void> sendConnect() {
        MqttMessageBuilders.ConnectBuilder builder = MqttMessageBuilders.connect()
                                                                        .clientId(clientId)
                                                                        .keepAlive(keepAliveSeconds)
                                                                        .cleanSession(cleanSession);

        if (protocolVersion == 5) {
            builder.protocolVersion(MqttVersion.MQTT_5);
        }

        if (username != null) {
            builder.username(username);
            if (password != null) {
                builder.password(password);
            }
        }

        if (willTopic != null) {
            byte[] willPayloadBytes = null;
            if (willPayload != null && willPayload.readableBytes() > 0) {
                willPayloadBytes = new byte[willPayload.readableBytes()];
                willPayload.getBytes(willPayload.readerIndex(), willPayloadBytes);
            }
            builder.willTopic(willTopic)
                   .willMessage(willPayloadBytes)
                   .willQoS(willQos)
                   .willRetain(willRetain);
        }

        MqttConnectMessage connectMessage = builder.build();

        return send(connectMessage)
                .then(connAckSink.asMono()
                                 .timeout(Duration.ofSeconds(10))
                                 .flatMap(connAck -> {
                                     if (connAck
                                             .variableHeader()
                                             .connectReturnCode() == MqttConnectReturnCode.CONNECTION_ACCEPTED) {
                                         connected.set(true);
                                         reconnectAttempt.set(0);
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
                handlerMono = handlerMono.then(entry.getValue().handler.apply(publishing));
            }
        }

        // 全局处理器
        if (publishingHandler != null) {
            handlerMono = handlerMono.then(publishingHandler.apply(publishing));
        }

        // 自动确认
        if (autoAck && msg.fixedHeader().qosLevel() != MqttQoS.AT_MOST_ONCE) {
            handlerMono = handlerMono.then(publishing.acknowledge());
        }

        return handlerMono.doFinally(signal -> publishing.release());
    }

    private Mono<Void> handlePubAck(MqttPubAckMessage msg) {
        int messageId = msg.variableHeader().messageId();
        Sinks.Empty<Void> sink = pendingPubAck.remove(messageId);
        if (sink != null) {
            sink.tryEmitEmpty();
        }
        return Mono.empty();
    }

    private Mono<Void> handlePubRec(MqttMessage msg) {
        int messageId = ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();
        Sinks.Empty<Void> sink = pendingPubRec.remove(messageId);
        if (sink != null) {
            sink.tryEmitEmpty();
        }

        // 发送 PUBREL
        MqttMessage pubRel = new MqttMessage(
                new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                MqttMessageIdVariableHeader.from(messageId)
        );

        Sinks.Empty<Void> compSink = Sinks.empty();
        pendingPubComp.put(messageId, compSink);

        return send(pubRel);
    }

    private Mono<Void> handlePubRel(MqttMessage msg) {
        int messageId = ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();

        // 发送 PUBCOMP
        MqttMessage pubComp = new MqttMessage(
                new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_MOST_ONCE, false, 0),
                MqttMessageIdVariableHeader.from(messageId)
        );

        return send(pubComp);
    }

    private Mono<Void> handlePubComp(MqttMessage msg) {
        int messageId = ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();
        Sinks.Empty<Void> sink = pendingPubComp.remove(messageId);
        if (sink != null) {
            sink.tryEmitEmpty();
        }
        return Mono.empty();
    }

    private Mono<Void> handleSubAck(MqttSubAckMessage msg) {
        int messageId = msg.variableHeader().messageId();
        Sinks.Empty<Void> sink = pendingSubAck.remove(messageId);
        if (sink != null) {
            sink.tryEmitEmpty();
        }
        return Mono.empty();
    }

    private Mono<Void> handleUnsubAck(MqttMessage msg) {
        int messageId = ((MqttMessageIdVariableHeader) msg.variableHeader()).messageId();
        Sinks.Empty<Void> sink = pendingUnsubAck.remove(messageId);
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
        if (!connected.compareAndSet(true, false)) {
            return;
        }

        if (closed.get()) {
            closeSink.tryEmitEmpty();
            return;
        }

        // 尝试重连
        attemptReconnect();
    }

    private void attemptReconnect() {
        if (!reconnecting.compareAndSet(false, true)) {
            return;
        }

        int attempt = reconnectAttempt.incrementAndGet();

        connAckSink = Sinks.one();

        reconnectStrategy.nextDelay(attempt, null)
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
                                     reconnecting.set(false);
                                     log.info("Reconnected successfully");
                                 },
                                 error -> {
                                     reconnecting.set(false);
                                     log.log(Level.WARNING, "Reconnect failed: " + error.getMessage());
                                     attemptReconnect();
                                 }
                         );
    }

    private Mono<Void> resubscribeIfNeeded() {
        if (!autoResubscribe || activeSubscriptions.isEmpty()) {
            return Mono.empty();
        }

        return Flux.fromIterable(activeSubscriptions)
                   .flatMap(sub -> doSubscribe(sub.topic, sub.qos))
                   .then();
    }

    @Override
    public Mono<Void> publish(String topic, ByteBuf payload, MqttQoS qos, boolean retain) {
        return Mono.defer(() -> {
            if (!connected.get()) {
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
                pendingPubAck.put(messageId, sink);
                return send(publishMessage)
                        .then(sink.asMono())
                        .timeout(Duration.ofSeconds(30))
                        .doOnError(e -> pendingPubAck.remove(messageId));
            } else {
                // QoS 2
                Sinks.Empty<Void> recSink = Sinks.empty();
                pendingPubRec.put(messageId, recSink);
                return send(publishMessage)
                        .then(recSink.asMono())
                        .then(Mono.defer(() -> {
                            Sinks.Empty<Void> compSink = pendingPubComp.get(messageId);
                            return compSink != null ? compSink.asMono() : Mono.empty();
                        }))
                        .timeout(Duration.ofSeconds(30))
                        .doOnError(e -> {
                            pendingPubRec.remove(messageId);
                            pendingPubComp.remove(messageId);
                        });
            }
        });
    }

    @Override
    public Disposable subscribe(String topic, MqttQoS qos, Function<MqttClientPublishing, Mono<Void>> handler) {
        SubscriptionHandler subHandler = new SubscriptionHandler(topic, qos, handler);
        subscriptionHandlers.put(topic, subHandler);
        activeSubscriptions.add(new SubscriptionInfo(topic, qos));

        Disposable.Composite composite = Disposables.composite();

        composite.add(doSubscribe(topic, qos).subscribe());
        composite.add(() -> {
            subscriptionHandlers.remove(topic);
            activeSubscriptions.removeIf(s -> s.topic.equals(topic));
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
        pendingSubAck.put(messageId, sink);

        return send(subscribeMessage)
                .then(sink.asMono())
                .timeout(Duration.ofSeconds(10))
                .doOnError(e -> pendingSubAck.remove(messageId));
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
            activeSubscriptions.add(new SubscriptionInfo(topic, MqttQoS.AT_MOST_ONCE));
        }

        MqttSubscribeMessage subscribeMessage = subscribeBuilder.build();

        Sinks.Empty<Void> sink = Sinks.empty();
        pendingSubAck.put(messageId, sink);

        return send(subscribeMessage)
                .then(sink.asMono())
                .timeout(Duration.ofSeconds(10))
                .doOnError(e -> pendingSubAck.remove(messageId));
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
            activeSubscriptions.removeIf(s -> s.topic.equals(topic));
        }

        MqttUnsubscribeMessage unsubscribeMessage = unsubscribeBuilder.build();

        Sinks.Empty<Void> sink = Sinks.empty();
        pendingUnsubAck.put(messageId, sink);

        return send(unsubscribeMessage)
                .then(sink.asMono())
                .timeout(Duration.ofSeconds(10))
                .doOnError(e -> pendingUnsubAck.remove(messageId));
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
        return connected.get() && connection != null && connection.channel().isActive();
    }

    @Override
    public Mono<Void> onClose() {
        return closeSink.asMono();
    }

    @Override
    public Mono<Void> close() {
        return Mono.defer(() -> {
            if (!closed.compareAndSet(false, true)) {
                return Mono.empty();
            }
            connected.set(false);
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
            if (!connected.get()) {
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
        return clientId;
    }

    @Override
    public MqttQoS getQos() {
        return qos;
    }

    private Mono<Void> send(MqttMessage message) {
        return connection.outbound()
                         .sendObject(Mono.just(message))
                         .then();
    }

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

    private static class SubscriptionInfo {
        final String topic;
        final MqttQoS qos;

        SubscriptionInfo(String topic, MqttQoS qos) {
            this.topic = topic;
            this.qos = qos;
        }
    }

    public static class MqttConnectionException extends RuntimeException {
        public MqttConnectionException(String message) {
            super(message);
        }
    }
}
