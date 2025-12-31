/*
 * Copyright 2025 JetLinks https://www.jetlinks.cn
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
import org.junit.jupiter.api.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.Connection;
import reactor.netty.DisposableServer;
import reactor.netty.tcp.TcpClient;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MQTT Server 单元测试 - 纯响应式实现
 *
 * @author PengyuDeng
 */
class MqttServerTest {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 21883;
    private static final int MAX_MESSAGE_SIZE = 65536;
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private DisposableServer server;
    private Connection clientConnection;

    @AfterEach
    void tearDown() {
        if (clientConnection != null && !clientConnection.isDisposed()) {
            clientConnection.dispose();
        }
        if (server != null && !server.isDisposed()) {
            server.disposeNow();
        }
    }

    // ==================== 辅助方法 ====================

    /**
     * 响应式创建 MQTT 客户端并连接
     */
    private Mono<Connection> createAndConnectClient(String clientId) {
        return createAndConnectClient(clientId, null, null);
    }

    /**
     * 响应式创建带认证信息的 MQTT 客户端并连接
     */
    private Mono<Connection> createAndConnectClient(String clientId, String username, String password) {
        Sinks.One<Connection> connectionSink = Sinks.one();

        TcpClient.create()
                 .host(HOST)
                 .port(PORT)
                 .doOnConnected(c -> {
                     c.addHandlerFirst("mqtt-encoder", MqttEncoder.INSTANCE);
                     c.addHandlerFirst("mqtt-decoder", new MqttDecoder(MAX_MESSAGE_SIZE));
                 })
                 .handle((inbound, outbound) -> {
                     MqttMessageBuilders.ConnectBuilder connectBuilder = MqttMessageBuilders.connect()
                                                                                            .clientId(clientId)
                                                                                            .cleanSession(true)
                                                                                            .keepAlive(300);

                     if (username != null) {
                         connectBuilder.username(username);
                     }
                     if (password != null) {
                         connectBuilder.password(password.getBytes(StandardCharsets.UTF_8));
                     }

                     MqttConnectMessage connectMessage = connectBuilder.build();

                     // 监听 CONNACK
                     inbound.receiveObject()
                            .cast(MqttMessage.class)
                            .filter(msg -> msg.fixedHeader().messageType() == MqttMessageType.CONNACK)
                            .next()
                            .subscribe(msg -> {
                                MqttConnAckMessage connAck = (MqttConnAckMessage) msg;
                                if (connAck
                                        .variableHeader()
                                        .connectReturnCode() == MqttConnectReturnCode.CONNECTION_ACCEPTED) {
                                    // 获取底层 Connection
                                    if (inbound instanceof Connection) {
                                        connectionSink.tryEmitValue((Connection) inbound);
                                    }
                                } else {
                                    connectionSink.tryEmitError(new RuntimeException("连接被拒绝: " + connAck
                                            .variableHeader()
                                            .connectReturnCode()));
                                }
                            });

                     // 发送 CONNECT
                     return outbound.sendObject(Mono.just(connectMessage))
                                    .then(Mono.never());
                 })
                 .connectNow(); // 使用 connectNow() 同步连接，避免连接池问题

        return connectionSink.asMono().timeout(TIMEOUT);
    }

    /**
     * 响应式尝试连接并返回 CONNACK
     */
    private Mono<MqttConnAckMessage> tryConnect(String clientId) {
        Sinks.One<MqttConnAckMessage> connAckSink = Sinks.one();

        return TcpClient.create()
                        .host(HOST)
                        .port(PORT)
                        .doOnConnected(c -> {
                            c.addHandlerFirst("mqtt-encoder", MqttEncoder.INSTANCE);
                            c.addHandlerFirst("mqtt-decoder", new MqttDecoder(MAX_MESSAGE_SIZE));
                        })
                        .handle((inbound, outbound) -> {
                            MqttConnectMessage connectMessage = MqttMessageBuilders.connect()
                                                                                   .clientId(clientId)
                                                                                   .cleanSession(true)
                                                                                   .keepAlive(300)
                                                                                   .build();

                            // 监听 CONNACK
                            inbound.receiveObject()
                                   .cast(MqttMessage.class)
                                   .filter(msg -> msg.fixedHeader().messageType() == MqttMessageType.CONNACK)
                                   .next()
                                   .cast(MqttConnAckMessage.class)
                                   .subscribe(connAckSink::tryEmitValue, connAckSink::tryEmitError);

                            // 发送 CONNECT，然后等待
                            return outbound.sendObject(Mono.just(connectMessage))
                                           .then(Mono.never());
                        })
                        .connect()
                        .doOnNext(conn -> clientConnection = conn)
                        .then(connAckSink.asMono().timeout(TIMEOUT));
    }

    /**
     * 响应式发布消息
     */
    private Mono<Void> publish(Connection conn, String topic, String payload, MqttQoS qos, AtomicInteger messageIdGen) {
        int messageId = qos == MqttQoS.AT_MOST_ONCE ? 0 : messageIdGen.getAndIncrement() & 0xFFFF;
        if (messageId == 0 && qos != MqttQoS.AT_MOST_ONCE) {
            messageId = messageIdGen.getAndIncrement() & 0xFFFF;
        }

        ByteBuf payloadBuf = Unpooled.wrappedBuffer(payload.getBytes(StandardCharsets.UTF_8));

        MqttPublishMessage publishMessage = new MqttPublishMessage(
                new MqttFixedHeader(MqttMessageType.PUBLISH, false, qos, false, 0),
                new MqttPublishVariableHeader(topic, messageId),
                payloadBuf
        );

        return conn.outbound().sendObject(Mono.just(publishMessage)).then();
    }

    /**
     * 响应式订阅主题
     */
    private Mono<Void> subscribe(Connection conn, String topic, MqttQoS qos, AtomicInteger messageIdGen) {
        int messageId = messageIdGen.getAndIncrement() & 0xFFFF;
        if (messageId == 0) {
            messageId = messageIdGen.getAndIncrement() & 0xFFFF;
        }

        MqttSubscribeMessage subscribeMessage = MqttMessageBuilders.subscribe()
                                                                   .messageId(messageId)
                                                                   .addSubscription(qos, topic)
                                                                   .build();

        return conn.outbound().sendObject(Mono.just(subscribeMessage)).then();
    }

    /**
     * 响应式取消订阅
     */
    private Mono<Void> unsubscribe(Connection conn, String topic, AtomicInteger messageIdGen) {
        int messageId = messageIdGen.getAndIncrement() & 0xFFFF;
        if (messageId == 0) {
            messageId = messageIdGen.getAndIncrement() & 0xFFFF;
        }

        MqttUnsubscribeMessage unsubscribeMessage = MqttMessageBuilders.unsubscribe()
                                                                       .messageId(messageId)
                                                                       .addTopicFilter(topic)
                                                                       .build();

        return conn.outbound().sendObject(Mono.just(unsubscribeMessage)).then();
    }

    /**
     * 响应式发送断开连接消息
     */
    private Mono<Void> disconnect(Connection conn) {
        MqttMessage disconnectMessage = new MqttMessage(
                new MqttFixedHeader(MqttMessageType.DISCONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0)
        );
        return conn.outbound().sendObject(Mono.just(disconnectMessage)).then()
                   .doFinally(signal -> conn.dispose());
    }

    // ==================== 配置验证测试 ====================

    @Test
    void testPortValidation() {
        assertThrows(IllegalArgumentException.class, () -> MqttServer.create().port(-1));
        assertThrows(IllegalArgumentException.class, () -> MqttServer.create().port(65536));
        assertDoesNotThrow(() -> MqttServer.create().port(0));
        assertDoesNotThrow(() -> MqttServer.create().port(65535));
    }

    @Test
    void testMaxMessageSizeValidation() {
        assertThrows(IllegalArgumentException.class, () -> MqttServer.create().maxMessageSize(0));
        assertThrows(IllegalArgumentException.class, () -> MqttServer.create().maxMessageSize(-1));
        assertDoesNotThrow(() -> MqttServer.create().maxMessageSize(1));
    }

    @Test
    void testIdleTimeoutValidation() {
        assertThrows(IllegalArgumentException.class, () ->
                MqttServer.create().idleTimeout(Duration.ofSeconds(-1)));
        assertDoesNotThrow(() -> MqttServer.create().idleTimeout(null));
        assertDoesNotThrow(() -> MqttServer.create().idleTimeout(Duration.ZERO));
    }

    @Test
    void testWorkerCountValidation() {
        assertThrows(IllegalArgumentException.class, () -> MqttServer.create().workerCount(0));
        assertThrows(IllegalArgumentException.class, () -> MqttServer.create().workerCount(-1));
        assertDoesNotThrow(() -> MqttServer.create().workerCount(1));
    }

    @Test
    void testSoBacklogValidation() {
        assertThrows(IllegalArgumentException.class, () -> MqttServer.create().soBacklog(0));
        assertThrows(IllegalArgumentException.class, () -> MqttServer.create().soBacklog(-1));
        assertDoesNotThrow(() -> MqttServer.create().soBacklog(1));
    }

    @Test
    void testWriteBufferWaterMarkValidation() {
        assertThrows(IllegalArgumentException.class, () ->
                MqttServer.create().writeBufferWaterMark(0, 100));
        assertThrows(IllegalArgumentException.class, () ->
                MqttServer.create().writeBufferWaterMark(100, 0));
        assertThrows(IllegalArgumentException.class, () ->
                MqttServer.create().writeBufferWaterMark(100, 50)); // low > high
        assertDoesNotThrow(() -> MqttServer.create().writeBufferWaterMark(50, 100));
        assertDoesNotThrow(() -> MqttServer.create().writeBufferWaterMark(100, 100)); // low == high
    }

    @Test
    void testHostValidation() {
        assertThrows(IllegalArgumentException.class, () -> MqttServer.create().host(null));
        assertThrows(IllegalArgumentException.class, () -> MqttServer.create().host(""));
        assertThrows(IllegalArgumentException.class, () -> MqttServer.create().host("   "));
        assertDoesNotThrow(() -> MqttServer.create().host("0.0.0.0"));
    }

    // ==================== 连接测试 ====================

    @Test
    void testClientConnect() {
        Sinks.One<String> clientIdSink = Sinks.one();

        server = MqttServer.create()
                           .host(HOST)
                           .port(PORT)
                           .handle(connection -> {
                               clientIdSink.tryEmitValue(connection.getClientId());
                               return connection.listener(new NoOpListener()).accept();
                           })
                           .bindNow();

        String clientId = createAndConnectClient("test-client-1")
                .doOnSuccess(conn -> clientConnection = conn)
                .then(clientIdSink.asMono())
                .block(TIMEOUT);

        assertEquals("test-client-1", clientId);

        if (clientConnection != null) {
            disconnect(clientConnection).block(TIMEOUT);
        }
    }

    @Test
    void testClientReject() {
        server = MqttServer.create()
                           .host(HOST)
                           .port(PORT)
                           .handle(connection -> connection.reject(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD))
                           .bindNow();

        MqttConnAckMessage connAck = tryConnect("test-client").block(TIMEOUT);

        assertEquals(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD,
                     connAck.variableHeader().connectReturnCode());
    }

    @Test
    void testAuthInfo() {
        Sinks.One<MqttAuth> authSink = Sinks.one();

        server = MqttServer.create()
                           .host(HOST)
                           .port(PORT)
                           .handle(connection -> {
                               authSink.tryEmitValue(connection.getAuth());
                               return connection.listener(new NoOpListener()).accept();
                           })
                           .bindNow();

        MqttAuth auth = createAndConnectClient("test-client", "testuser", "testpass")
                .doOnSuccess(conn -> clientConnection = conn)
                .then(authSink.asMono())
                .block(TIMEOUT);

        assertEquals("testuser", auth.getUsername());
        assertEquals("testpass", auth.getPassword());

        if (clientConnection != null) {
            disconnect(clientConnection).block(TIMEOUT);
        }
    }

    // ==================== 消息发布测试 ====================

    @Test
    void testPublishQoS0() {
        Sinks.One<MqttPublishing> messageSink = Sinks.one();

        server = MqttServer.create()
                           .host(HOST)
                           .port(PORT)
                           .handle(connection -> connection.listener(new MqttMessageListener() {
                               @Override
                               public Mono<Void> onPublish(MqttPublishing message) {
                                   messageSink.tryEmitValue(message);
                                   return Mono.empty();
                               }

                               @Override
                               public Mono<Void> onSubscribe(MqttSubscription subscription) {
                                   return Mono.empty();
                               }

                               @Override
                               public Mono<Void> onUnsubscribe(MqttUnSubscription unsubscription) {
                                   return Mono.empty();
                               }

                               @Override
                               public Mono<Void> onDisconnect(MqttConnection connection) {
                                   return Mono.empty();
                               }
                           }).accept())
                           .bindNow();

        AtomicInteger messageIdGen = new AtomicInteger(1);

        createAndConnectClient("test-client")
                .doOnSuccess(conn -> clientConnection = conn)
                .flatMap(conn -> publish(conn, "test/topic", "hello", MqttQoS.AT_MOST_ONCE, messageIdGen))
                .then(messageSink.asMono())
                .doOnSuccess(msg -> {
                    assertEquals("test/topic", msg.getTopic());
                    assertEquals("hello", msg.getPayload().toString(StandardCharsets.UTF_8));
                });


        if (clientConnection != null) {
            disconnect(clientConnection).block(TIMEOUT);
        }
    }

    @Test
    void testPublishQoS1() {
        Sinks.One<Integer> qosSink = Sinks.one();

        server = MqttServer.create()
                           .host(HOST)
                           .port(PORT)
                           .handle(connection -> connection.listener(new MqttMessageListener() {
                               @Override
                               public Mono<Void> onPublish(MqttPublishing message) {
                                   qosSink.tryEmitValue(message.getQosLevel());
                                   return Mono.empty();
                               }

                               @Override
                               public Mono<Void> onSubscribe(MqttSubscription subscription) {
                                   return Mono.empty();
                               }

                               @Override
                               public Mono<Void> onUnsubscribe(MqttUnSubscription unsubscription) {
                                   return Mono.empty();
                               }

                               @Override
                               public Mono<Void> onDisconnect(MqttConnection connection) {
                                   return Mono.empty();
                               }
                           }).accept())
                           .bindNow();

        AtomicInteger messageIdGen = new AtomicInteger(1);

        createAndConnectClient("test-client")
                .doOnSuccess(conn -> clientConnection = conn)
                .flatMap(conn -> publish(conn, "test/topic", "hello", MqttQoS.AT_LEAST_ONCE, messageIdGen))
                .then(qosSink.asMono())
                .doOnSuccess(qos -> assertEquals(1, qos));


        if (clientConnection != null) {
            disconnect(clientConnection).block(TIMEOUT);
        }
    }

    @Test
    void testPublishQoS2() {
        Sinks.One<Integer> qosSink = Sinks.one();

        server = MqttServer.create()
                           .host(HOST)
                           .port(PORT)
                           .handle(connection -> connection.listener(new MqttMessageListener() {
                               @Override
                               public Mono<Void> onPublish(MqttPublishing message) {
                                   qosSink.tryEmitValue(message.getQosLevel());
                                   return Mono.empty();
                               }

                               @Override
                               public Mono<Void> onSubscribe(MqttSubscription subscription) {
                                   return Mono.empty();
                               }

                               @Override
                               public Mono<Void> onUnsubscribe(MqttUnSubscription unsubscription) {
                                   return Mono.empty();
                               }

                               @Override
                               public Mono<Void> onDisconnect(MqttConnection connection) {
                                   return Mono.empty();
                               }
                           }).accept())
                           .bindNow();

        AtomicInteger messageIdGen = new AtomicInteger(1);

        Integer qos = createAndConnectClient("test-client")
                .doOnSuccess(conn -> clientConnection = conn)
                .flatMap(conn -> publish(conn, "test/topic", "hello", MqttQoS.EXACTLY_ONCE, messageIdGen))
                .then(qosSink.asMono())
                .block(TIMEOUT);

        assertEquals(2, qos);

        if (clientConnection != null) {
            disconnect(clientConnection).block(TIMEOUT);
        }
    }

    // ==================== 订阅测试 ====================

    @Test
    void testSubscribe() {
        Sinks.One<Boolean> subscribedSink = Sinks.one();

        server = MqttServer.create()
                           .host(HOST)
                           .port(PORT)
                           .handle(connection -> connection.listener(new MqttMessageListener() {
                               @Override
                               public Mono<Void> onPublish(MqttPublishing message) {
                                   return Mono.empty();
                               }

                               @Override
                               public Mono<Void> onSubscribe(MqttSubscription subscription) {
                                   subscribedSink.tryEmitValue(true);
                                   return Mono.empty();
                               }

                               @Override
                               public Mono<Void> onUnsubscribe(MqttUnSubscription unsubscription) {
                                   return Mono.empty();
                               }

                               @Override
                               public Mono<Void> onDisconnect(MqttConnection connection) {
                                   return Mono.empty();
                               }
                           }).accept())
                           .bindNow();

        AtomicInteger messageIdGen = new AtomicInteger(1);

        Boolean subscribed = createAndConnectClient("test-client")
                .doOnSuccess(conn -> clientConnection = conn)
                .flatMap(conn -> subscribe(conn, "test/#", MqttQoS.AT_LEAST_ONCE, messageIdGen))
                .then(subscribedSink.asMono())
                .block(TIMEOUT);

        assertTrue(subscribed);

        if (clientConnection != null) {
            disconnect(clientConnection).block(TIMEOUT);
        }
    }

    @Test
    void testUnsubscribe() {
        Sinks.One<Boolean> subscribedSink = Sinks.one();
        Sinks.One<Boolean> unsubscribedSink = Sinks.one();

        server = MqttServer.create()
                           .host(HOST)
                           .port(PORT)
                           .handle(connection -> connection.listener(new MqttMessageListener() {
                               @Override
                               public Mono<Void> onPublish(MqttPublishing message) {
                                   return Mono.empty();
                               }

                               @Override
                               public Mono<Void> onSubscribe(MqttSubscription subscription) {
                                   subscribedSink.tryEmitValue(true);
                                   return Mono.empty();
                               }

                               @Override
                               public Mono<Void> onUnsubscribe(MqttUnSubscription unsubscription) {
                                   unsubscribedSink.tryEmitValue(true);
                                   return Mono.empty();
                               }

                               @Override
                               public Mono<Void> onDisconnect(MqttConnection connection) {
                                   return Mono.empty();
                               }
                           }).accept())
                           .bindNow();

        AtomicInteger messageIdGen = new AtomicInteger(1);

        Boolean unsubscribed = createAndConnectClient("test-client")
                .doOnSuccess(conn -> clientConnection = conn)
                .flatMap(conn -> subscribe(conn, "test/#", MqttQoS.AT_LEAST_ONCE, messageIdGen)
                        .then(subscribedSink.asMono())
                        .then(unsubscribe(conn, "test/#", messageIdGen)))
                .then(unsubscribedSink.asMono())
                .block(TIMEOUT);

        assertTrue(unsubscribed);

        if (clientConnection != null) {
            disconnect(clientConnection).block(TIMEOUT);
        }
    }

    // ==================== 断开连接测试 ====================

    @Test
    void testDisconnectCallback() {
        Sinks.One<Boolean> connectedSink = Sinks.one();
        Sinks.One<Boolean> disconnectedSink = Sinks.one();

        server = MqttServer.create()
                           .host(HOST)
                           .port(PORT)
                           .handle(connection -> {
                               connectedSink.tryEmitValue(true);
                               return connection.listener(new MqttMessageListener() {
                                   @Override
                                   public Mono<Void> onPublish(MqttPublishing message) {
                                       return Mono.empty();
                                   }

                                   @Override
                                   public Mono<Void> onSubscribe(MqttSubscription subscription) {
                                       return Mono.empty();
                                   }

                                   @Override
                                   public Mono<Void> onUnsubscribe(MqttUnSubscription unsubscription) {
                                       return Mono.empty();
                                   }

                                   @Override
                                   public Mono<Void> onDisconnect(MqttConnection conn) {
                                       disconnectedSink.tryEmitValue(true);
                                       return Mono.empty();
                                   }
                               }).accept();
                           })
                           .bindNow();

        Boolean disconnected = createAndConnectClient("test-client")
                .flatMap(conn -> {
                    clientConnection = conn;
                    return connectedSink.asMono()
                                        .then(disconnect(conn))
                                        .then(disconnectedSink.asMono());
                })
                .block(TIMEOUT);

        assertTrue(disconnected);
    }

    // ==================== 多消息测试 ====================

    @Test
    void testMultipleMessages() {
        int expectedCount = 100;
        AtomicInteger messageCount = new AtomicInteger(0);
        Sinks.One<Integer> completeSink = Sinks.one();

        server = MqttServer.create()
                           .host(HOST)
                           .port(PORT)
                           .handle(connection -> connection.listener(new MqttMessageListener() {
                               @Override
                               public Mono<Void> onPublish(MqttPublishing message) {
                                   int count = messageCount.incrementAndGet();
                                   if (count >= expectedCount) {
                                       completeSink.tryEmitValue(count);
                                   }
                                   return Mono.empty();
                               }

                               @Override
                               public Mono<Void> onSubscribe(MqttSubscription subscription) {
                                   return Mono.empty();
                               }

                               @Override
                               public Mono<Void> onUnsubscribe(MqttUnSubscription unsubscription) {
                                   return Mono.empty();
                               }

                               @Override
                               public Mono<Void> onDisconnect(MqttConnection connection) {
                                   return Mono.empty();
                               }
                           }).accept())
                           .bindNow();

        AtomicInteger messageIdGen = new AtomicInteger(1);

        Integer count = createAndConnectClient("test-client")
                .doOnSuccess(conn -> clientConnection = conn)
                .flatMap(conn -> Flux.range(0, expectedCount)
                                     .flatMap(i -> publish(conn, "test/topic", "msg-" + i, MqttQoS.AT_LEAST_ONCE, messageIdGen))
                                     .then())
                .then(completeSink.asMono())
                .block(TIMEOUT);

        assertEquals(expectedCount, count);

        if (clientConnection != null) {
            disconnect(clientConnection).block(TIMEOUT);
        }
    }

    // ==================== 辅助类 ====================

    /**
     * 空操作监听器，用于不需要处理消息的测试
     */
    private static class NoOpListener implements MqttMessageListener {
        @Override
        public Mono<Void> onPublish(MqttPublishing message) {
            return Mono.empty();
        }

        @Override
        public Mono<Void> onSubscribe(MqttSubscription subscription) {
            return Mono.empty();
        }

        @Override
        public Mono<Void> onUnsubscribe(MqttUnSubscription unsubscription) {
            return Mono.empty();
        }

        @Override
        public Mono<Void> onDisconnect(MqttConnection connection) {
            return Mono.empty();
        }
    }
}
