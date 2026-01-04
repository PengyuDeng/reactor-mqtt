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

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.jetlinks.reactor.mqtt.server.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MQTT Client 单元测试
 *
 * @author PengyuDeng
 */
class MqttClientTest {

    private DisposableServer server;
    private ClientConnection clientConnection;
    private static final int PORT = 11883;
    private static final String VALID_USERNAME = "testuser";
    private static final String VALID_PASSWORD = "testpass";
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

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
    private Mono<DisposableServer> startServer() {
        return MqttServer.create()
                         .host("127.0.0.1")
                         .port(PORT)
                         .handle(connection -> {
                             // 认证校验逻辑
                             MqttAuth auth = connection.getAuth();
                             if (auth.hasAuth()) {
                                 // 有认证信息时，校验账号密码
                                 if (!VALID_USERNAME.equals(auth.getUsername()) ||
                                         !VALID_PASSWORD.equals(auth.getPassword())) {
                                     return connection.reject(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD);
                                 }
                             }
                             // 无认证信息或认证通过，接受连接
                             connection.listener(new MqttMessageListener() {
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
                             });
                             return connection.accept();
                         })
                         .bind()
                         .map(s -> {
                             server = s;
                             return s;
                         });
    }

    @Test
    void testConnect() {
        StepVerifier.create(startServer()
                                    .then(MqttClient.create()
                                                    .host("127.0.0.1")
                                                    .port(PORT)
                                                    .clientId("test-client-1")
                                                    .connect()
                                                    .doOnNext(conn -> clientConnection = conn)
                                                    .flatMap(conn -> {
                                                        assertNotNull(conn);
                                                        assertTrue(conn.isConnected());
                                                        assertEquals("test-client-1", conn.getClientId());
                                                        return conn.disconnect()
                                                                   .thenReturn(conn);
                                                    }))
                    )
                    .assertNext(conn -> assertFalse(conn.isConnected()))
                    .expectComplete()
                    .verify(TIMEOUT);
    }

    @Test
    void testConnectWithAuth() {
        StepVerifier.create(startServer()
                                    .then(MqttClient.create()
                                                    .host("127.0.0.1")
                                                    .port(PORT)
                                                    .clientId("test-client-auth")
                                                    .auth(VALID_USERNAME, VALID_PASSWORD)
                                                    .connect()
                                                    .doOnNext(conn -> clientConnection = conn)
                                                    .flatMap(conn -> {
                                                        assertNotNull(conn);
                                                        assertTrue(conn.isConnected());
                                                        return conn.disconnect();
                                                    }))
                    )
                    .expectComplete()
                    .verify(TIMEOUT);
    }

    @Test
    void testConnectWithBadAuth() {
        StepVerifier.create(startServer()
                                    .then(MqttClient.create()
                                                    .host("127.0.0.1")
                                                    .port(PORT)
                                                    .clientId("test-client-bad-auth")
                                                    .auth("wronguser", "wrongpass")
                                                    .connect())
                    )
                    .expectError()
                    .verify(TIMEOUT);
    }

    @Test
    void testPublishQoS0() {
        StepVerifier.create(startServer()
                                    .then(MqttClient.create()
                                                    .host("127.0.0.1")
                                                    .port(PORT)
                                                    .clientId("test-publisher-qos0")
                                                    .connect()
                                                    .doOnNext(conn -> clientConnection = conn)
                                                    .flatMap(conn ->
                                                                     // QoS 0 发送即忘
                                                                     conn.publish("/test/topic",
                                                                                  Unpooled.wrappedBuffer("Hello QoS0".getBytes(StandardCharsets.UTF_8)),
                                                                                  MqttQoS.AT_MOST_ONCE)
                                                                         .then(conn.disconnect())
                                                    ))
                    )
                    .expectComplete()
                    .verify(TIMEOUT);
    }

    @Test
    void testPublishQoS1() {
        StepVerifier.create(startServer()
                                    .then(MqttClient.create()
                                                    .host("127.0.0.1")
                                                    .port(PORT)
                                                    .clientId("test-publisher-qos1")
                                                    .connect()
                                                    .doOnNext(conn -> clientConnection = conn)
                                                    .flatMap(conn ->
                                                                     // QoS 1 等待 PUBACK
                                                                     conn.publish("/test/topic",
                                                                                  Unpooled.wrappedBuffer("Hello QoS1".getBytes(StandardCharsets.UTF_8)),
                                                                                  MqttQoS.AT_LEAST_ONCE)
                                                                         .then(conn.disconnect())
                                                    ))
                    )
                    .expectComplete()
                    .verify(TIMEOUT);
    }

    @Test
    void testSubscribe() {
        AtomicInteger messageCount = new AtomicInteger(0);

        StepVerifier.create(startServer()
                                    .then(MqttClient.create()
                                                    .host("127.0.0.1")
                                                    .port(PORT)
                                                    .clientId("test-subscriber")
                                                    .connect()
                                                    .doOnNext(conn -> clientConnection = conn)
                                                    .flatMap(conn -> {
                                                        Disposable subscription = conn.subscribe("/test/topic", MqttQoS.AT_LEAST_ONCE, msg -> {
                                                            messageCount.incrementAndGet();
                                                            return Mono.empty();
                                                        });
                                                        assertNotNull(subscription);
                                                        // 取消订阅
                                                        subscription.dispose();
                                                        return conn.disconnect();
                                                    }))
                    )
                    .expectComplete()
                    .verify(TIMEOUT);
    }

    @Test
    void testSubscribeMultipleTopics() {
        StepVerifier.create(startServer()
                                    .then(MqttClient.create()
                                                    .host("127.0.0.1")
                                                    .port(PORT)
                                                    .clientId("test-multi-subscriber")
                                                    .connect()
                                                    .doOnNext(conn -> clientConnection = conn)
                                                    .flatMap(conn ->
                                                                     conn.subscribe("/topic1", "/topic2", "/topic3")
                                                                         .then(conn.unsubscribe("/topic1", "/topic2"))
                                                                         .then(conn.disconnect())
                                                    ))
                    )
                    .expectComplete()
                    .verify(TIMEOUT);
    }

    @Test
    void testReceiveFlux() {
        StepVerifier.create(startServer()
                                    .then(MqttClient.create()
                                                    .host("127.0.0.1")
                                                    .port(PORT)
                                                    .clientId("test-receiver")
                                                    .connect()
                                                    .doOnNext(conn -> clientConnection = conn)
                                                    .flatMap(conn -> {
                                                        // 获取消息流（高级 API）
                                                        assertNotNull(conn.receive());
                                                        return conn.disconnect();
                                                    }))
                    )
                    .expectComplete()
                    .verify(TIMEOUT);
    }

    @Test
    void testOnClose() {
        StepVerifier.create(startServer()
                                    .then(MqttClient.create()
                                                    .host("127.0.0.1")
                                                    .port(PORT)
                                                    .clientId("test-close")
                                                    .connect()
                                                    .doOnNext(conn -> clientConnection = conn)
                                                    .flatMap(conn -> {
                                                        assertTrue(conn.isConnected());
                                                        // 关闭连接并等待 onClose 事件完成
                                                        return conn.close()
                                                                   .then(conn.onClose())
                                                                   .thenReturn(conn);
                                                    }))
                    )
                    .assertNext(conn -> assertFalse(conn.isConnected()))
                    .expectComplete()
                    .verify(TIMEOUT);
    }

    @Test
    void testReconnectStrategy() {
        // 测试各种重连策略
        ReconnectStrategy none = ReconnectStrategy.none();
        StepVerifier.create(none.nextDelay(1, null))
                    .expectComplete()
                    .verify();

        ReconnectStrategy fixed = ReconnectStrategy.fixedDelay(Duration.ofSeconds(5));
        StepVerifier.create(fixed.nextDelay(1, null))
                    .expectNext(Duration.ofSeconds(5))
                    .expectComplete()
                    .verify();
        StepVerifier.create(fixed.nextDelay(10, null))
                    .expectNext(Duration.ofSeconds(5))
                    .expectComplete()
                    .verify();

        ReconnectStrategy exponential = ReconnectStrategy.exponentialBackoff(
                Duration.ofSeconds(1), Duration.ofSeconds(30));
        StepVerifier.create(exponential.nextDelay(1, null))
                    .expectNext(Duration.ofSeconds(1))
                    .expectComplete()
                    .verify();
        StepVerifier.create(exponential.nextDelay(2, null))
                    .expectNext(Duration.ofSeconds(2))
                    .expectComplete()
                    .verify();
        StepVerifier.create(exponential.nextDelay(3, null))
                    .expectNext(Duration.ofSeconds(4))
                    .expectComplete()
                    .verify();

        // 验证最大值限制
        StepVerifier.create(exponential.nextDelay(100, null))
                    .expectNext(Duration.ofSeconds(30))
                    .expectComplete()
                    .verify();

        // 带最大重试次数
        ReconnectStrategy limited = ReconnectStrategy.exponentialBackoff(
                Duration.ofSeconds(1), Duration.ofSeconds(30), 3);
        StepVerifier.create(limited.nextDelay(1, null))
                    .expectNextCount(1)
                    .expectComplete()
                    .verify();
        StepVerifier.create(limited.nextDelay(3, null))
                    .expectNextCount(1)
                    .expectComplete()
                    .verify();
        StepVerifier.create(limited.nextDelay(4, null))
                    .expectComplete()
                    .verify();
    }

    @Test
    void testClientIdGeneration() {
        StepVerifier.create(startServer()
                                    .then(MqttClient.create()
                                                    .host("127.0.0.1")
                                                    .port(PORT)
                                                    // 不设置 clientId，自动生成
                                                    .connect()
                                                    .doOnNext(conn -> clientConnection = conn)
                                                    .flatMap(conn -> {
                                                        assertNotNull(conn.getClientId());
                                                        assertTrue(conn.getClientId().startsWith("reactor-mqtt-"));
                                                        return conn.disconnect();
                                                    }))
                    )
                    .expectComplete()
                    .verify(TIMEOUT);
    }

    @Test
    void testProtocolVersion() {
        StepVerifier.create(startServer()
                                    .then(
                                            // MQTT 3.1.1
                                            MqttClient.create()
                                                      .host("127.0.0.1")
                                                      .port(PORT)
                                                      .clientId("test-mqtt311")
                                                      .protocolVersion(MqttVersion.MQTT_3_1_1)
                                                      .connect()
                                                      .doOnNext(conn -> clientConnection = conn)
                                                      .flatMap(conn -> {
                                                          assertTrue(conn.isConnected());
                                                          return conn.disconnect();
                                                      })
                                                      // MQTT 5.0
                                                      .then(MqttClient.create()
                                                                      .host("127.0.0.1")
                                                                      .port(PORT)
                                                                      .clientId("test-mqtt5")
                                                                      .protocolVersion(MqttVersion.MQTT_5)
                                                                      .connect()
                                                                      .doOnNext(conn -> clientConnection = conn)
                                                                      .flatMap(conn -> {
                                                                          assertTrue(conn.isConnected());
                                                                          return conn.disconnect();
                                                                      }))
                                    )
                    )
                    .expectComplete()
                    .verify(TIMEOUT);
    }

    @Test
    void testQos() {
        // 配置默认 QoS 为 AT_LEAST_ONCE
        StepVerifier.create(startServer()
                                    .then(MqttClient.create()
                                                    .host("127.0.0.1")
                                                    .port(PORT)
                                                    .clientId("test-default-qos")
                                                    .qos(MqttQoS.AT_LEAST_ONCE)
                                                    .connect()
                                                    .doOnNext(conn -> clientConnection = conn)
                                                    .flatMap(conn -> {
                                                        assertEquals(MqttQoS.AT_LEAST_ONCE, conn.getQos());
                                                        // 使用默认 QoS 发布消息（不指定 QoS）
                                                        return conn.publish("/test/topic",
                                                                            Unpooled.wrappedBuffer("Hello".getBytes(StandardCharsets.UTF_8)))
                                                                   .then(conn.disconnect());
                                                    }))
                    )
                    .expectComplete()
                    .verify(TIMEOUT);
    }

    @Test
    void testSubscriptionManagerWithDifferentTopics() {
        AtomicInteger topic1Count = new AtomicInteger(0);
        AtomicInteger topic2Count = new AtomicInteger(0);
        AtomicInteger wildcardCount = new AtomicInteger(0);
        AtomicInteger sensor1Count = new AtomicInteger(0);
        AtomicInteger sensor2Count = new AtomicInteger(0);

        // 创建支持消息回环（loopback）的服务器
        Mono<DisposableServer> server = MqttServer.create()
                .host("127.0.0.1")
                .port(PORT)
                .handle(connection -> {
                    connection.listener(new MqttMessageListener() {
                        @Override
                        public Mono<Void> onPublish(ServerReceivedPublish message) {
                            // 将消息回传给发送者（模拟订阅匹配）
                            // retain 消息以避免引用计数问题
                            io.netty.util.ReferenceCountUtil.retain(message.getOrigin());
                            return connection.publish(message.getOrigin());
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
                    });
                    return connection.accept();
                })
                .bind()
                .map(s -> {
                    this.server = s;
                    return s;
                });

        StepVerifier.create(server
                                    .then(MqttClient.create()
                                                    .host("127.0.0.1")
                                                    .port(PORT)
                                                    .clientId("test-subscription-manager")
                                                    .connect())
                                    .flatMap(conn -> {
                                        clientConnection = conn;

                                        Flux<ClientReceivedPublish> receive = conn.receive();


                                        // 订阅精确主题 - 不同 QoS
                                        Disposable sub1 = conn.subscribe("/home/temperature", MqttQoS.AT_MOST_ONCE, msg -> {
                                            topic1Count.incrementAndGet();
                                            String payload = msg.getPayload().toString(StandardCharsets.UTF_8);
                                            assertTrue(payload.contains("temperature"));
                                            return msg.acknowledge();
                                        });

                                        Disposable sub2 = conn.subscribe("/home/humidity", MqttQoS.AT_LEAST_ONCE, msg -> {
                                            topic2Count.incrementAndGet();
                                            String payload = msg.getPayload().toString(StandardCharsets.UTF_8);
                                            assertTrue(payload.contains("humidity"));
                                            return msg.acknowledge();
                                        });

                                        // 订阅通配符主题 - 匹配所有 /sensor/# 主题
                                        Disposable sub3 = conn.subscribe("/sensor/#", MqttQoS.EXACTLY_ONCE, msg -> {
                                            wildcardCount.incrementAndGet();
                                            return msg.acknowledge();
                                        });

                                        // 订阅单层通配符 - 匹配 /sensor/+/data
                                        Disposable sub4 = conn.subscribe("/sensor/+/data", MqttQoS.AT_LEAST_ONCE, msg -> {
                                            String topic = msg.getTopic();
                                            if (topic.contains("sensor1")) {
                                                sensor1Count.incrementAndGet();
                                            } else if (topic.contains("sensor2")) {
                                                sensor2Count.incrementAndGet();
                                            }
                                            return msg.acknowledge();
                                        });

                                        // 等待订阅完成
                                        return Mono.delay(Duration.ofMillis(500))
                                                   .then(Mono.defer(() -> {
                                                       // 发布到不同主题
                                                       return Mono.when(
                                                               // 发布到 /home/temperature (应该被 sub1 接收)
                                                               conn.publish("/home/temperature",
                                                                           Unpooled.wrappedBuffer("temperature: 25.5".getBytes(StandardCharsets.UTF_8)),
                                                                           MqttQoS.AT_MOST_ONCE),
                                                               // 发布到 /home/humidity (应该被 sub2 接收)
                                                               conn.publish("/home/humidity",
                                                                           Unpooled.wrappedBuffer("humidity: 60%".getBytes(StandardCharsets.UTF_8)),
                                                                           MqttQoS.AT_LEAST_ONCE),
                                                               // 发布到 /sensor/sensor1/data (应该被 sub3 和 sub4 接收)
                                                               conn.publish("/sensor/sensor1/data",
                                                                           Unpooled.wrappedBuffer("sensor1 data".getBytes(StandardCharsets.UTF_8)),
                                                                           MqttQoS.EXACTLY_ONCE),
                                                               // 发布到 /sensor/sensor2/data (应该被 sub3 和 sub4 接收)
                                                               conn.publish("/sensor/sensor2/data",
                                                                           Unpooled.wrappedBuffer("sensor2 data".getBytes(StandardCharsets.UTF_8)),
                                                                           MqttQoS.AT_LEAST_ONCE),
                                                               // 发布到 /sensor/other (只应该被 sub3 接收，不被 sub4 接收)
                                                               conn.publish("/sensor/other",
                                                                           Unpooled.wrappedBuffer("other data".getBytes(StandardCharsets.UTF_8)),
                                                                           MqttQoS.AT_LEAST_ONCE),
                                                               // 发布到不匹配的主题 (不应该被任何订阅接收)
                                                               conn.publish("/other/topic",
                                                                           Unpooled.wrappedBuffer("unmatched".getBytes(StandardCharsets.UTF_8)),
                                                                           MqttQoS.AT_MOST_ONCE)
                                                       );
                                                   }))
                                                   // 等待消息被接收和处理
                                                   .then(Mono.delay(Duration.ofMillis(500)))
                                                   .then(Mono.fromRunnable(() -> {
                                                       // 验证消息计数
                                                       assertEquals(1, topic1Count.get(), "/home/temperature 应该收到 1 条消息");
                                                       assertEquals(1, topic2Count.get(), "/home/humidity 应该收到 1 条消息");
                                                       assertEquals(3, wildcardCount.get(), "/sensor/# 应该收到 3 条消息");
                                                       assertEquals(1, sensor1Count.get(), "/sensor/sensor1/data 应该收到 1 条消息");
                                                       assertEquals(1, sensor2Count.get(), "/sensor/sensor2/data 应该收到 1 条消息");

                                                       // 清理订阅
                                                       sub1.dispose();
                                                       sub2.dispose();
                                                       sub3.dispose();
                                                       sub4.dispose();
                                                   }))
                                                   .then(conn.disconnect());
                                    })
                    )
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));
    }
}
