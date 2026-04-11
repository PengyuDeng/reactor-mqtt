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
package org.jetlinks.reactor.mqtt;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.jetlinks.reactor.mqtt.client.ClientConnection;
import org.jetlinks.reactor.mqtt.client.MqttClient;
import org.jetlinks.reactor.mqtt.server.MqttServer;
import org.jetlinks.reactor.mqtt.server.ServerConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.DisposableServer;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MQTT 客户端和服务端集成测试
 *
 * @author PengyuDeng
 */
class MqttIntegrationTest {

    private DisposableServer server;
    private ClientConnection client;
    private static final int TEST_PORT = 11885;

    @BeforeEach
    void setUp() {
        // 启动服务端
        server = MqttServer.create()
                           .host("127.0.0.1")
                           .port(TEST_PORT)
                           .handle(ServerConnection::accept)
                           .bindNow();
    }

    @AfterEach
    void tearDown() {
        // 清理客户端
        if (client != null && client.isAlive()) {
            try {
                client.disconnect().block(Duration.ofSeconds(2));
            } catch (Exception e) {
                // 忽略
            }
        }

        // 清理服务端
        if (server != null && !server.isDisposed()) {
            server.disposeNow(Duration.ofSeconds(2));
        }
    }

    @Test
    @Timeout(15)
    void testClientServerConnection() throws InterruptedException {
        // 测试客户端和服务端的基本连接
        CountDownLatch connectionLatch = new CountDownLatch(1);
        AtomicReference<String> connectedClientId = new AtomicReference<>();

        // 创建带连接处理器的服务端
        if (server != null) {
            server.disposeNow();
        }

        server = MqttServer.create()
                           .port(TEST_PORT)
                           .handle(connection -> {
                               connectedClientId.set(connection.getClientId());
                               connectionLatch.countDown();
                               return connection.accept();
                           })
                           .bindNow();

        // 客户端连接
        client = MqttClient.create()
                           .host("127.0.0.1")
                           .port(TEST_PORT)
                           .clientId("integration-test-client")
                           .connectNow();

        // 等待连接建立
        assertTrue(connectionLatch.await(5, TimeUnit.SECONDS));
        assertEquals("integration-test-client", connectedClientId.get());
        assertTrue(client.isAlive());
    }

    @Test
    @Timeout(15)
    void testMultipleMessages() {
        int messageCount = 10;
        CountDownLatch messageLatch = new CountDownLatch(messageCount);
        AtomicInteger serverReceivedCount = new AtomicInteger(0);
        AtomicReference<String> lastPayload = new AtomicReference<>();

        if (server != null) {
            server.disposeNow();
        }

        server = MqttServer.create()
                           .host("127.0.0.1")
                           .port(TEST_PORT)
                           .handle(connection -> {
                               connection.handlePublishing(msg -> {
                                   serverReceivedCount.incrementAndGet();
                                   lastPayload.set(msg.message().payload().toString(StandardCharsets.UTF_8));
                                   messageLatch.countDown();
                                   return Mono.empty();
                               });
                               return connection.accept();
                           })
                           .bindNow();

        client = MqttClient.create()
                           .host("127.0.0.1")
                           .port(TEST_PORT)
                           .clientId("multi-msg-test")
                           .connectNow();

        StepVerifier.create(
                            Flux.range(0, messageCount)
                                .flatMap(i -> {
                                    String message = "Message " + i;
                                    return client.publish("test/multi",
                                                          Unpooled.wrappedBuffer(message.getBytes(StandardCharsets.UTF_8)),
                                                          MqttQoS.AT_MOST_ONCE);
                                })
                    )
                    .verifyComplete();

        assertTrue(await(messageLatch));
        assertEquals(messageCount, serverReceivedCount.get());
        assertEquals("Message 9", lastPayload.get());
    }

    @Test
    @Timeout(15)
    void testUnsubscribe() throws InterruptedException {
        CountDownLatch messageLatch = new CountDownLatch(1);
        CountDownLatch unsubscribeLatch = new CountDownLatch(1);
        AtomicInteger messageCount = new AtomicInteger(0);
        AtomicReference<ServerConnection> serverConnection = new AtomicReference<>();

        if (server != null) {
            server.disposeNow();
        }

        server = MqttServer.create()
                           .host("127.0.0.1")
                           .port(TEST_PORT)
                           .handle(connection -> {
                               serverConnection.set(connection);
                               connection.handleUnsubscribe(unsub -> {
                                   unsubscribeLatch.countDown();
                                   return Mono.empty();
                               });
                               return connection.accept();
                           })
                           .bindNow();

        client = MqttClient.create()
                           .host("127.0.0.1")
                           .port(TEST_PORT)
                           .clientId("unsub-test")
                           .connectNow();

        // 订阅
        var subscription = client.subscribe("test/unsub", msg -> {
            messageCount.incrementAndGet();
            messageLatch.countDown();
            return Mono.empty();
        });

        publishFromServer(serverConnection.get(), "test/unsub", "Message 1", MqttQoS.AT_MOST_ONCE);

        assertTrue(messageLatch.await(5, TimeUnit.SECONDS));
        assertEquals(1, messageCount.get());

        subscription.dispose();
        assertTrue(unsubscribeLatch.await(5, TimeUnit.SECONDS));

        publishFromServer(serverConnection.get(), "test/unsub", "Message 2", MqttQoS.AT_MOST_ONCE);

        Thread.sleep(300);

        assertEquals(1, messageCount.get());
    }

    @Test
    @Timeout(15)
    void testWildcardSubscription() throws InterruptedException {
        CountDownLatch messageLatch = new CountDownLatch(2);
        AtomicInteger receivedCount = new AtomicInteger(0);
        AtomicReference<ServerConnection> serverConnection = new AtomicReference<>();

        if (server != null) {
            server.disposeNow();
        }

        server = MqttServer.create()
                           .host("127.0.0.1")
                           .port(TEST_PORT)
                           .handle(connection -> {
                               serverConnection.set(connection);
                               return connection.accept();
                           })
                           .bindNow();

        client = MqttClient.create()
                           .host("127.0.0.1")
                           .port(TEST_PORT)
                           .clientId("wildcard-test")
                           .connectNow();

        // 使用通配符订阅
        client.subscribe("sensor/+/temperature", msg -> {
            receivedCount.incrementAndGet();
            messageLatch.countDown();
            return Mono.empty();
        });

        publishFromServer(serverConnection.get(), "sensor/room1/temperature", "20", MqttQoS.AT_MOST_ONCE);
        publishFromServer(serverConnection.get(), "sensor/room2/temperature", "22", MqttQoS.AT_MOST_ONCE);
        publishFromServer(serverConnection.get(), "sensor/room2/humidity", "60", MqttQoS.AT_MOST_ONCE);

        assertTrue(messageLatch.await(5, TimeUnit.SECONDS));
        assertEquals(2, receivedCount.get());
    }


    @Test
    @Timeout(15)
    void testPublishSubscribeFinal() {
        String topic = "test/qos1";
        String payloadStr = "Hello MQTT";
        Sinks.One<String> payloadSink = Sinks.one();

        if (server != null) {
            server.disposeNow();
        }

        server = MqttServer.create()
                           .host("127.0.0.1")
                           .port(TEST_PORT)
                           .handle(connection -> {
                               connection.handlePublishing(msg -> {
                                   payloadSink.tryEmitValue(msg.message().payload().toString(StandardCharsets.UTF_8));
                                   return Mono.empty();
                               });
                               return connection.accept();
                           })
                           .bindNow();

        Mono<Void> testFlow = MqttClient.create()
                                        .host("127.0.0.1")
                                        .port(TEST_PORT)
                                        .clientId("pub-sub-test")
                                        .connect()
                                        .flatMap(connection -> {
                                            return connection.publish(topic,
                                                                      Unpooled.wrappedBuffer(payloadStr.getBytes(StandardCharsets.UTF_8)),
                                                                      MqttQoS.AT_LEAST_ONCE,
                                                                      false)
                                                              .then(payloadSink.asMono())
                                                              .doOnNext(content -> assertEquals(payloadStr, content))
                                                              .then()
                                                              .doFinally(sig -> {
                                                                  connection.close().subscribe();
                                                              });
                                        });

        StepVerifier.create(testFlow)
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));
    }

    @Test
    @Timeout(15)
    void testConnectionLifecycleReactive() {
        Mono<Void> lifecycleTest = MqttClient.create()
                                             .host("127.0.0.1")
                                             .port(TEST_PORT)
                                             .clientId("lifecycle-test")
                                            .connect()
                                            .flatMap(connection -> {
                                                assertTrue(connection.isAlive(), "连接应该是存活状态");
                                                return connection.disconnect()
                                                                  .then(connection.onClose())
                                                                  .doOnSuccess(v -> assertFalse(connection.isAlive(), "断开后连接不应存活"));
                                            });

        StepVerifier.create(lifecycleTest)
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));
    }

    private boolean await(CountDownLatch latch) {
        try {
            return latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e);
            return false;
        }
    }

    private void publishFromServer(ServerConnection connection, String topic, String payload, MqttQoS qos) {
        assertInstanceOf(org.jetlinks.reactor.mqtt.server.DefaultServerConnection.class, connection);
        ((org.jetlinks.reactor.mqtt.server.DefaultServerConnection) connection)
                .publish(topic,
                         Unpooled.wrappedBuffer(payload.getBytes(StandardCharsets.UTF_8)),
                         qos,
                         false)
                .block(Duration.ofSeconds(5));
    }
}
