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
import java.util.Arrays;
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
import java.util.stream.Collectors;

/**
 * MQTT 客户端连接实现
 *
 * @author PengyuDeng
 */
public class DefaultMqttClientConnection implements MqttClientConnection {

    private static final Logger log = Logger.getLogger(DefaultMqttClientConnection.class.getName());

    private volatile Connection connection;
    private final String clientId;
    private final String username;
    private final byte[] password;
    private final int keepAliveSeconds;
    private final boolean cleanSession;
    private final int protocolVersion;

    // 遗言
    private final String willTopic;
    private final ByteBuf willPayload;
    private final MqttQoS willQos;
    private final boolean willRetain;

    // 消息处理
    private final Function<MqttClientPublishing, Mono<Void>> publishingHandler;
    private final boolean autoAck;
    private final MqttQoS defaultQos;

    // 重连
    private final ReconnectStrategy reconnectStrategy;
    private final boolean autoResubscribe;
    private final Supplier<TcpClient> tcpClientSupplier;
    private final AtomicInteger reconnectAttempt = new AtomicInteger(0);
    private final AtomicBoolean reconnecting = new AtomicBoolean(false);

    // 状态
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Sinks.Empty<Void> closeSink = Sinks.empty();
    private final Sinks.One<MqttConnAckMessage> connAckSink = Sinks.one();

    // 消息 ID 生成
    private final AtomicInteger messageIdGenerator = new AtomicInteger(0);

    // Pending 消息（等待确认）
    private final Map<Integer, Sinks.Empty<Void>> pendingPubAck = new ConcurrentHashMap<>();
    private final Map<Integer, Sinks.Empty<Void>> pendingPubRec = new ConcurrentHashMap<>();
    private final Map<Integer, Sinks.Empty<Void>> pendingPubComp = new ConcurrentHashMap<>();
    private final Map<Integer, Sinks.Empty<Void>> pendingSubAck = new ConcurrentHashMap<>();
    private final Map<Integer, Sinks.Empty<Void>> pendingUnsubAck = new ConcurrentHashMap<>();

    // 订阅管理
    private final Map<String, SubscriptionHandler> subscriptionHandlers = new ConcurrentHashMap<>();
    private final List<SubscriptionInfo> activeSubscriptions = new CopyOnWriteArrayList<>();

    // 消息流
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
                                       MqttQoS defaultQos,
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
        this.defaultQos = defaultQos;
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
        connAckSink.tryEmitValue(msg);
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

        reconnectStrategy.nextDelay(attempt, null)
                         .flatMap(delay -> Mono.delay(delay)
                                               .then(tcpClientSupplier.get().connect())
                                               .flatMap(conn -> {
                                                   this.connection = conn;
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
        return defaultQos;
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
