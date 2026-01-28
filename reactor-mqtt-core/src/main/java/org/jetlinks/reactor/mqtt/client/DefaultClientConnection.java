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

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            STATE = lookup.findVarHandle(DefaultClientConnection.class, "state", int.class);
            MESSAGE_ID_GENERATOR = lookup.findVarHandle(DefaultClientConnection.class, "messageIdGenerator", int.class);
            CONNECTION = lookup.findVarHandle(DefaultClientConnection.class, "connection", Connection.class);
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
     * 关闭完成信号,用于 onClose() 方法（延迟创建以节省内存）
     */
    private volatile Sinks.Empty<Void> closeSink;

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
            connAckSink = Sinks.one();
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
                       if (log.isLoggable(Level.WARNING)) {
                           log.log(Level.WARNING, "Failed to add MQTT codec: " + e.getMessage(), e);
                       }
                       return Mono.empty();
                   });
    }

    private void setupConnectionHandlers() {
        Connection conn = (Connection) CONNECTION.get(this);
        conn.inbound()
            .receiveObject()
            .cast(MqttMessage.class)
            .flatMap(this::handleMessage)
            .subscribe(
                    null,
                    this::handleError,
                    this::handleComplete
            );

        conn.onDispose(() -> {
            if (!hasFlag(CLOSED)) {
                handleDisconnect();
            }
        });
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
                .then(connAckSink.asMono()
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
        Function<ClientReceivedPublish, Mono<Void>> publishingHandler = config.getPublishingHandler();
        if (publishingHandler != null) {
            handlerMono = handlerMono.then(publishingHandler.apply(publishing));
        }

        // 自动确认
        if (config.isAutoAck() && msg.fixedHeader().qosLevel() != MqttQoS.AT_MOST_ONCE) {
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
            log.log(Level.WARNING, "Connection error for client " + config.getClientId() + ": " + error.getMessage(), error);
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

        config.getReconnectStrategy()
              .nextDelay(attempt, null)
              .switchIfEmpty(Mono.defer(() -> {
                  if (log.isLoggable(Level.WARNING)) {
                      log.log(Level.WARNING, () -> "Max reconnect attempts reached for client " + config.getClientId() + ", closing connection");
                  }
                  clearFlag(RECONNECTING);
                  close().subscribe();
                  return Mono.empty();
              }))
              .flatMap(delay -> Mono.delay(delay)
                                    .then(tcpClientSupplier.get().connect())
                                    .flatMap(conn -> {
                                        CONNECTION.set(this, conn);
                                        return ensureMqttCodec()
                                                .then(Mono.fromRunnable(this::setupConnectionHandlers))
                                                .then(sendConnect());
                                    })
                                    .then(resubscribeIfNeeded())
              )
              .subscribe(
                      v -> clearFlag(RECONNECTING),
                      error -> {
                          clearFlag(RECONNECTING);
                          if (log.isLoggable(Level.WARNING)) {
                              log.log(Level.WARNING, () -> "Reconnect failed for client " + config.getClientId() + ": " + error.getMessage());
                          }
                          attemptReconnect();
                      },
                      () -> {
                          // 重连成功
                          clearFlag(RECONNECTING);
                          if (log.isLoggable(Level.INFO)) {
                              log.log(Level.INFO, () -> "Reconnect successful for client " + config.getClientId());
                          }
                          if (reconnectSink.currentSubscriberCount() > 0) {
                              reconnectSink.tryEmitNext(attempt);
                          }
                      }
              );
    }

    private Mono<Void> resubscribeIfNeeded() {
        if (!config.isAutoResubscribe()) {
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
                        .timeout(config.getPublishTimeout())
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
                        .timeout(config.getPublishTimeout())
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
                .timeout(config.getSubscribeTimeout())
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
                .timeout(config.getUnsubscribeTimeout())
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
        Connection conn = (Connection) CONNECTION.get(this);
        return hasFlag(CONNECTED) && conn != null && conn.channel().isActive();
    }

    @Override
    public Mono<Void> onClose() {
        // 延迟创建 closeSink
        if (closeSink == null) {
            synchronized (this) {
                if (closeSink == null) {
                    closeSink = Sinks.empty();
                }
            }
        }
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

            Connection conn = (Connection) CONNECTION.get(this);
            if (conn != null) {
                conn.dispose();
            }
            if (closeSink != null) {
                closeSink.tryEmitEmpty();
            }
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

        if (log.isLoggable(Level.FINE)) {
            log.fine("Starting heartbeat for client " + config.getClientId() + " with interval " + intervalSeconds + "s");
        }

        heartbeatTimer = Flux.interval(Duration.ofSeconds(intervalSeconds), Schedulers.parallel())
                             .flatMap(tick -> sendPing())
                             .subscribe(null,
                                        error -> {
                                            if (log.isLoggable(Level.WARNING)) {
                                                log.log(Level.WARNING, "Heartbeat error for client " + config.getClientId(), error);
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
                log.fine("Stopped heartbeat for client " + config.getClientId());
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
        return send(MqttMessage.PINGREQ)
                .doOnSuccess(v -> {
                    if (log.isLoggable(Level.FINEST)) {
                        log.finest("Sending PINGREQ for client " + config.getClientId());
                    }
                })
                .doOnError(error -> {
                    if (log.isLoggable(Level.WARNING)) {
                        log.log(Level.WARNING, "Failed to send PINGREQ for client " + config.getClientId(), error);
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

    public static class MqttConnectionException extends RuntimeException {
        public MqttConnectionException(String message) {
            super(message);
        }
    }
}
