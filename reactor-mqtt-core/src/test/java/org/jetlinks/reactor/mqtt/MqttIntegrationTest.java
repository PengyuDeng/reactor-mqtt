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
import java.util.Collections;
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
        // 测试多条消息的发布订阅 - 使用响应式方式
        int messageCount = 10;
        AtomicInteger receivedCount = new AtomicInteger(0);
        Sinks.One<Integer> completionSink = Sinks.one();

        client = MqttClient.create()
                           .host("127.0.0.1")
                           .port(TEST_PORT)
                           .clientId("multi-msg-test")
                           .connectNow();

        // 订阅
        client.subscribe("test/multi", msg -> {
            int count = receivedCount.incrementAndGet();
            if (count == messageCount) {
                completionSink.tryEmitValue(count);
            }
            return Mono.empty();
        });

        // 发布多条消息并等待接收
        StepVerifier.create(
                            Flux.range(0, messageCount)
                                .flatMap(i -> {
                                    String message = "Message " + i;
                                    return client.publish("test/multi",
                                                          Unpooled.wrappedBuffer(message.getBytes(StandardCharsets.UTF_8)),
                                                          MqttQoS.AT_MOST_ONCE);
                                })
                                .then(Mono.delay(Duration.ofMillis(100)))
                                .then(completionSink.asMono())
                    )
                    .expectNext(messageCount)
                    .verifyComplete();

        assertEquals(messageCount, receivedCount.get());
    }

    @Test
    @Timeout(15)
    void testUnsubscribe() throws InterruptedException {
        // 测试取消订阅
        CountDownLatch messageLatch = new CountDownLatch(1);
        AtomicInteger messageCount = new AtomicInteger(0);

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

        Thread.sleep(500);

        // 发送第一条消息
        client.publish("test/unsub",
                       Unpooled.wrappedBuffer("Message 1".getBytes(StandardCharsets.UTF_8)),
                       MqttQoS.AT_MOST_ONCE)
              .block();

        assertTrue(messageLatch.await(5, TimeUnit.SECONDS));
        assertEquals(1, messageCount.get());

        // 取消订阅
        subscription.dispose();
        Thread.sleep(500);

        // 发送第二条消息（不应该收到）
        client.publish("test/unsub",
                       Unpooled.wrappedBuffer("Message 2".getBytes(StandardCharsets.UTF_8)),
                       MqttQoS.AT_MOST_ONCE)
              .block();

        Thread.sleep(1000);

        // 仍然只收到一条消息
        assertEquals(1, messageCount.get());
    }

    @Test
    @Timeout(15)
    void testWildcardSubscription() throws InterruptedException {
        // 测试通配符订阅
        CountDownLatch messageLatch = new CountDownLatch(2);
        AtomicInteger receivedCount = new AtomicInteger(0);

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

        Thread.sleep(500);

        // 发布到不同的子主题
        client.publish("sensor/room1/temperature",
                       Unpooled.wrappedBuffer("20".getBytes(StandardCharsets.UTF_8)))
              .block();

        client.publish("sensor/room2/temperature",
                       Unpooled.wrappedBuffer("22".getBytes(StandardCharsets.UTF_8)))
              .block();

        assertTrue(messageLatch.await(5, TimeUnit.SECONDS));
        assertEquals(2, receivedCount.get());
    }


    @Test
    @Timeout(15)
    void testPublishSubscribeFinal() {
        String topic = "test/topic";
        String payloadStr = "Hello MQTT";
        Sinks.One<String> messageSink = Sinks.one();

        Mono<Void> testFlow = MqttClient.create()
                                        .host("127.0.0.1")
                                        .port(TEST_PORT)
                                        .clientId("pub-sub-test")
                                        .connect()
                                        .flatMap(connection -> {
                                            connection.subscribe(Collections.singleton(topic), MqttQoS.AT_LEAST_ONCE, msg -> {
                                                String content = msg.getPayload().toString(StandardCharsets.UTF_8);
                                                messageSink.tryEmitValue(content);
                                                return Mono.empty();
                                            });

                                            Mono<Void> publishAndForget = Mono.delay(Duration.ofMillis(500))
                                                                              .then(connection.publish(topic,
                                                                                                       Unpooled.wrappedBuffer(payloadStr.getBytes(StandardCharsets.UTF_8)),
                                                                                                       MqttQoS.AT_LEAST_ONCE, false))
                                                                              .onErrorResume(e -> {
                                                                                  messageSink.tryEmitError(e);
                                                                                  return Mono.empty();
                                                                              });

                                            return messageSink.asMono()
                                                              .doOnSubscribe(s -> publishAndForget.subscribe())
                                                              .delayElement(Duration.ofMillis(200)) // <--- 给 PUBACK 留出发送时间
                                                              .then()
                                                              .doFinally(sig -> {
                                                                  System.out.println(">>> 正在清理连接... 信号: " + sig);
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
}
