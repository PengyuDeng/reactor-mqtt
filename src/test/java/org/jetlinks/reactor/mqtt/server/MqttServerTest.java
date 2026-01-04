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

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.*;
import org.jetlinks.reactor.mqtt.client.MqttClient;
import org.jetlinks.reactor.mqtt.client.ClientConnection;
import org.junit.jupiter.api.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.DisposableServer;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MQTT Server 单元测试 - 纯响应式实现
 *
 * @author PengyuDeng
 */
class MqttServerTest {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 21883;
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private DisposableServer server;
    private ClientConnection clientConnection;

    @AfterEach
    void tearDown() {
        if (clientConnection != null) {
            clientConnection.disconnect().block(Duration.ofSeconds(2));
        }
        if (server != null && !server.isDisposed()) {
            server.disposeNow();
        }
    }

    /**
     * 创建并启动 MQTT 服务器
     */
    private Mono<DisposableServer> startServer(Function<ServerConnection, Mono<Void>> handler) {
        return MqttServer.create()
                         .host(HOST)
                         .port(PORT)
                         .handle(handler)
                         .bind()
                         .map(s -> {
                             server = s;
                             return s;
                         });
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

        StepVerifier.create(
            startServer(connection -> {
                clientIdSink.tryEmitValue(connection.getClientId());
                return connection.listener(new NoOpListener()).accept();
            })
                    .then(MqttClient.create()
                                    .host(HOST)
                                    .port(PORT)
                                    .clientId("test-client-1")
                                    .connect()
                                    .doOnNext(conn -> clientConnection = conn)
                                    .flatMap(conn -> clientIdSink.asMono()))
        )
        .assertNext(clientId -> assertEquals("test-client-1", clientId))
        .expectComplete()
        .verify(TIMEOUT);
    }

    @Test
    void testClientReject() {
        StepVerifier.create(
            startServer(connection -> connection.reject(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD))
                    .then(MqttClient.create()
                                    .host(HOST)
                                    .port(PORT)
                                    .clientId("test-client")
                                    .connect())
        )
        .expectError()
        .verify(TIMEOUT);
    }

    @Test
    void testAuthInfo() {
        Sinks.One<MqttAuth> authSink = Sinks.one();

        StepVerifier.create(
            startServer(connection -> {
                authSink.tryEmitValue(connection.getAuth());
                return connection.listener(new NoOpListener()).accept();
            })
                    .then(MqttClient.create()
                                    .host(HOST)
                                    .port(PORT)
                                    .clientId("test-client")
                                    .auth("testuser", "testpass")
                                    .connect()
                                    .doOnNext(conn -> clientConnection = conn)
                                    .flatMap(conn -> authSink.asMono()))
        )
        .assertNext(auth -> {
            assertEquals("testuser", auth.getUsername());
            assertEquals("testpass", auth.getPassword());
        })
        .expectComplete()
        .verify(TIMEOUT);
    }

    // ==================== 消息发布测试 ====================

    @Test
    void testPublishQoS0() {
        Sinks.One<ServerReceivedPublish> messageSink = Sinks.one();

        StepVerifier.create(
            startServer(connection -> connection.listener(new MqttMessageListener() {
                @Override
                public Mono<Void> onPublish(ServerReceivedPublish message) {
                    messageSink.tryEmitValue(message);
                    return Mono.empty();
                }

                @Override
                public Mono<Void> onSubscribe(MqttSubscription subscription) {
                    return Mono.empty();
                }

                @Override
                public Mono<Void> onUnsubscribe(MqttUnsubscription unsubscription) {
                    return Mono.empty();
                }

                @Override
                public Mono<Void> onDisconnect(ServerConnection connection) {
                    return Mono.empty();
                }
            }).accept())
                    .then(MqttClient.create()
                                    .host(HOST)
                                    .port(PORT)
                                    .clientId("test-client")
                                    .connect()
                                    .doOnNext(conn -> clientConnection = conn)
                                    .flatMap(conn -> conn.publish("test/topic",
                                                                   Unpooled.wrappedBuffer("hello".getBytes(StandardCharsets.UTF_8)),
                                                                   MqttQoS.AT_MOST_ONCE))
                                    .then(messageSink.asMono()))
        )
        .assertNext(msg -> {
            assertEquals("test/topic", msg.getTopic());
            assertEquals("hello", msg.getPayload().toString(StandardCharsets.UTF_8));
        })
        .expectComplete()
        .verify(TIMEOUT);
    }

    @Test
    void testPublishQoS1() {
        Sinks.One<Integer> qosSink = Sinks.one();

        StepVerifier.create(
            startServer(connection -> connection.listener(new MqttMessageListener() {
                @Override
                public Mono<Void> onPublish(ServerReceivedPublish message) {
                    qosSink.tryEmitValue(message.getQosLevel());
                    return Mono.empty();
                }

                @Override
                public Mono<Void> onSubscribe(MqttSubscription subscription) {
                    return Mono.empty();
                }

                @Override
                public Mono<Void> onUnsubscribe(MqttUnsubscription unsubscription) {
                    return Mono.empty();
                }

                @Override
                public Mono<Void> onDisconnect(ServerConnection connection) {
                    return Mono.empty();
                }
            }).accept())
                    .then(MqttClient.create()
                                    .host(HOST)
                                    .port(PORT)
                                    .clientId("test-client")
                                    .connect()
                                    .doOnNext(conn -> clientConnection = conn)
                                    .flatMap(conn -> conn.publish("test/topic",
                                                                   Unpooled.wrappedBuffer("hello".getBytes(StandardCharsets.UTF_8)),
                                                                   MqttQoS.AT_LEAST_ONCE))
                                    .then(qosSink.asMono()))
        )
        .assertNext(qos -> assertEquals(1, qos))
        .expectComplete()
        .verify(TIMEOUT);
    }

    @Test
    void testPublishQoS2() {
        Sinks.One<Integer> qosSink = Sinks.one();

        StepVerifier.create(
            startServer(connection -> connection.listener(new MqttMessageListener() {
                @Override
                public Mono<Void> onPublish(ServerReceivedPublish message) {
                    qosSink.tryEmitValue(message.getQosLevel());
                    return Mono.empty();
                }

                @Override
                public Mono<Void> onSubscribe(MqttSubscription subscription) {
                    return Mono.empty();
                }

                @Override
                public Mono<Void> onUnsubscribe(MqttUnsubscription unsubscription) {
                    return Mono.empty();
                }

                @Override
                public Mono<Void> onDisconnect(ServerConnection connection) {
                    return Mono.empty();
                }
            }).accept())
                    .then(MqttClient.create()
                                    .host(HOST)
                                    .port(PORT)
                                    .clientId("test-client")
                                    .connect()
                                    .doOnNext(conn -> clientConnection = conn)
                                    .flatMap(conn -> conn.publish("test/topic",
                                                                   Unpooled.wrappedBuffer("hello".getBytes(StandardCharsets.UTF_8)),
                                                                   MqttQoS.EXACTLY_ONCE))
                                    .then(qosSink.asMono()))
        )
        .assertNext(qos -> assertEquals(2, qos))
        .expectComplete()
        .verify(TIMEOUT);
    }

    // ==================== 订阅测试 ====================

    @Test
    void testSubscribe() {
        Sinks.One<Boolean> subscribedSink = Sinks.one();

        StepVerifier.create(
            startServer(connection -> connection.listener(new MqttMessageListener() {
                @Override
                public Mono<Void> onPublish(ServerReceivedPublish message) {
                    return Mono.empty();
                }

                @Override
                public Mono<Void> onSubscribe(MqttSubscription subscription) {
                    subscribedSink.tryEmitValue(true);
                    return Mono.empty();
                }

                @Override
                public Mono<Void> onUnsubscribe(MqttUnsubscription unsubscription) {
                    return Mono.empty();
                }

                @Override
                public Mono<Void> onDisconnect(ServerConnection connection) {
                    return Mono.empty();
                }
            }).accept())
                    .then(MqttClient.create()
                                    .host(HOST)
                                    .port(PORT)
                                    .clientId("test-client")
                                    .connect()
                                    .doOnNext(conn -> clientConnection = conn)
                                    .flatMap(conn -> conn.subscribe("test/#"))
                                    .then(subscribedSink.asMono()))
        )
        .assertNext(subscribed -> assertTrue(subscribed))
        .expectComplete()
        .verify(TIMEOUT);
    }

    @Test
    void testUnsubscribe() {
        Sinks.One<Boolean> subscribedSink = Sinks.one();
        Sinks.One<Boolean> unsubscribedSink = Sinks.one();

        StepVerifier.create(
            startServer(connection -> connection.listener(new MqttMessageListener() {
                @Override
                public Mono<Void> onPublish(ServerReceivedPublish message) {
                    return Mono.empty();
                }

                @Override
                public Mono<Void> onSubscribe(MqttSubscription subscription) {
                    subscribedSink.tryEmitValue(true);
                    return Mono.empty();
                }

                @Override
                public Mono<Void> onUnsubscribe(MqttUnsubscription unsubscription) {
                    unsubscribedSink.tryEmitValue(true);
                    return Mono.empty();
                }

                @Override
                public Mono<Void> onDisconnect(ServerConnection connection) {
                    return Mono.empty();
                }
            }).accept())
                    .then(MqttClient.create()
                                    .host(HOST)
                                    .port(PORT)
                                    .clientId("test-client")
                                    .connect()
                                    .doOnNext(conn -> clientConnection = conn)
                                    .flatMap(conn -> conn.subscribe("test/#")
                                                         .then(subscribedSink.asMono())
                                                         .then(conn.unsubscribe("test/#")))
                                    .then(unsubscribedSink.asMono()))
        )
        .assertNext(unsubscribed -> assertTrue(unsubscribed))
        .expectComplete()
        .verify(TIMEOUT);
    }

    // ==================== 断开连接测试 ====================

    @Test
    void testDisconnectCallback() {
        Sinks.One<Boolean> connectedSink = Sinks.one();
        Sinks.One<Boolean> disconnectedSink = Sinks.one();

        StepVerifier.create(
            startServer(connection -> {
                connectedSink.tryEmitValue(true);
                return connection.listener(new MqttMessageListener() {
                    @Override
                    public Mono<Void> onPublish(ServerReceivedPublish message) {
                        return Mono.empty();
                    }

                    @Override
                    public Mono<Void> onSubscribe(MqttSubscription subscription) {
                        return Mono.empty();
                    }

                    @Override
                    public Mono<Void> onUnsubscribe(MqttUnsubscription unsubscription) {
                        return Mono.empty();
                    }

                    @Override
                    public Mono<Void> onDisconnect(ServerConnection conn) {
                        disconnectedSink.tryEmitValue(true);
                        return Mono.empty();
                    }
                }).accept();
            })
                    .then(MqttClient.create()
                                    .host(HOST)
                                    .port(PORT)
                                    .clientId("test-client")
                                    .connect()
                                    .doOnNext(conn -> clientConnection = conn)
                                    .flatMap(conn -> connectedSink.asMono()
                                                                   .then(conn.disconnect())
                                                                   .then(disconnectedSink.asMono())))
        )
        .assertNext(disconnected -> assertTrue(disconnected))
        .expectComplete()
        .verify(TIMEOUT);
    }

    // ==================== 多消息测试 ====================

    @Test
    void testMultipleMessages() {
        int expectedCount = 100;
        AtomicInteger messageCount = new AtomicInteger(0);
        Sinks.One<Integer> completeSink = Sinks.one();

        StepVerifier.create(
            startServer(connection -> connection.listener(new MqttMessageListener() {
                @Override
                public Mono<Void> onPublish(ServerReceivedPublish message) {
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
                public Mono<Void> onUnsubscribe(MqttUnsubscription unsubscription) {
                    return Mono.empty();
                }

                @Override
                public Mono<Void> onDisconnect(ServerConnection connection) {
                    return Mono.empty();
                }
            }).accept())
                    .then(MqttClient.create()
                                    .host(HOST)
                                    .port(PORT)
                                    .clientId("test-client")
                                    .connect()
                                    .doOnNext(conn -> clientConnection = conn)
                                    .flatMap(conn -> Flux.range(0, expectedCount)
                                                         .flatMap(i -> conn.publish("test/topic",
                                                                                     Unpooled.wrappedBuffer(("msg-" + i).getBytes(StandardCharsets.UTF_8)),
                                                                                     MqttQoS.AT_LEAST_ONCE))
                                                         .then())
                                    .then(completeSink.asMono()))
        )
        .assertNext(count -> assertEquals(expectedCount, count))
        .expectComplete()
        .verify(TIMEOUT);
    }

    // ==================== 辅助类 ====================

    /**
     * 空操作监听器，用于不需要处理消息的测试
     */
    private static class NoOpListener implements MqttMessageListener {
        @Override
        public Mono<Void> onPublish(ServerReceivedPublish message) {
            return Mono.empty();
        }

        @Override
        public Mono<Void> onSubscribe(MqttSubscription subscription) {
            return Mono.empty();
        }

        @Override
        public Mono<Void> onUnsubscribe(MqttUnsubscription unsubscription) {
            return Mono.empty();
        }

        @Override
        public Mono<Void> onDisconnect(ServerConnection connection) {
            return Mono.empty();
        }
    }
}
