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
import org.junit.jupiter.api.*;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;

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

    private static DisposableServer server;
    private static final int PORT = 11883;
    private static final String VALID_USERNAME = "testuser";
    private static final String VALID_PASSWORD = "testpass";

    @BeforeAll
    static void startServer() {
        server = MqttServer.create()
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
                        public Mono<Void> onPublish(MqttPublishing message) {
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
                        public Mono<Void> onDisconnect(MqttConnection connection) {
                            return Mono.empty();
                        }
                    });
                    return connection.accept();
                })
                .bindNow();
    }

    @AfterAll
    static void stopServer() {
        if (server != null) {
            server.disposeNow();
        }
    }

    @Test
    void testConnect() {
        MqttClientConnection conn = MqttClient.create()
                .host("127.0.0.1")
                .port(PORT)
                .clientId("test-client-1")
                .connectNow(Duration.ofSeconds(5));

        assertNotNull(conn);
        assertTrue(conn.isConnected());
        assertEquals("test-client-1", conn.getClientId());

        conn.disconnect().block(Duration.ofSeconds(5));
        assertFalse(conn.isConnected());
    }

    @Test
    void testConnectWithAuth() {
        // 使用正确的账号密码连接
        MqttClientConnection conn = MqttClient.create()
                .host("127.0.0.1")
                .port(PORT)
                .clientId("test-client-auth")
                .auth(VALID_USERNAME, VALID_PASSWORD)
                .connectNow(Duration.ofSeconds(5));

        assertNotNull(conn);
        assertTrue(conn.isConnected());

        conn.disconnect().block(Duration.ofSeconds(5));
    }

    @Test
    void testConnectWithBadAuth() {
        // 使用错误的账号密码连接，应该失败
        assertThrows(Exception.class, () -> {
            MqttClient.create()
                    .host("127.0.0.1")
                    .port(PORT)
                    .clientId("test-client-bad-auth")
                    .auth("wronguser", "wrongpass")
                    .connectNow(Duration.ofSeconds(5));
        });
    }

    @Test
    void testPublishQoS0() {
        MqttClientConnection conn = MqttClient.create()
                .host("127.0.0.1")
                .port(PORT)
                .clientId("test-publisher-qos0")
                .connectNow(Duration.ofSeconds(5));

        // QoS 0 发送即忘
        conn.publish("/test/topic",
                Unpooled.wrappedBuffer("Hello QoS0".getBytes(StandardCharsets.UTF_8)),
                MqttQoS.AT_MOST_ONCE)
            .block(Duration.ofSeconds(5));

        conn.disconnect().block(Duration.ofSeconds(5));
    }

    @Test
    void testPublishQoS1() {
        MqttClientConnection conn = MqttClient.create()
                .host("127.0.0.1")
                .port(PORT)
                .clientId("test-publisher-qos1")
                .connectNow(Duration.ofSeconds(5));

        // QoS 1 等待 PUBACK
        conn.publish("/test/topic",
                Unpooled.wrappedBuffer("Hello QoS1".getBytes(StandardCharsets.UTF_8)),
                MqttQoS.AT_LEAST_ONCE)
            .block(Duration.ofSeconds(5));

        conn.disconnect().block(Duration.ofSeconds(5));
    }

    @Test
    void testSubscribe() {
        MqttClientConnection conn = MqttClient.create()
                .host("127.0.0.1")
                .port(PORT)
                .clientId("test-subscriber")
                .connectNow(Duration.ofSeconds(5));

        AtomicInteger messageCount = new AtomicInteger(0);

        Disposable subscription = conn.subscribe("/test/topic", MqttQoS.AT_LEAST_ONCE, msg -> {
            messageCount.incrementAndGet();
            return Mono.empty();
        });

        assertNotNull(subscription);

        // 取消订阅
        subscription.dispose();

        conn.disconnect().block(Duration.ofSeconds(5));
    }

    @Test
    void testSubscribeMultipleTopics() {
        MqttClientConnection conn = MqttClient.create()
                .host("127.0.0.1")
                .port(PORT)
                .clientId("test-multi-subscriber")
                .connectNow(Duration.ofSeconds(5));

        conn.subscribe("/topic1", "/topic2", "/topic3")
            .block(Duration.ofSeconds(5));

        conn.unsubscribe("/topic1", "/topic2")
            .block(Duration.ofSeconds(5));

        conn.disconnect().block(Duration.ofSeconds(5));
    }

    @Test
    void testReceiveFlux() {
        MqttClientConnection conn = MqttClient.create()
                .host("127.0.0.1")
                .port(PORT)
                .clientId("test-receiver")
                .connectNow(Duration.ofSeconds(5));

        // 获取消息流（高级 API）
        assertNotNull(conn.receive());

        conn.disconnect().block(Duration.ofSeconds(5));
    }

    @Test
    void testOnClose() {
        MqttClientConnection conn = MqttClient.create()
                .host("127.0.0.1")
                .port(PORT)
                .clientId("test-close")
                .connectNow(Duration.ofSeconds(5));

        assertTrue(conn.isConnected());

        // 关闭连接并等待 onClose 事件完成
        conn.close()
            .then(conn.onClose())
            .block(Duration.ofSeconds(5));

        assertFalse(conn.isConnected());
    }

    @Test
    void testReconnectStrategy() {
        // 测试各种重连策略
        ReconnectStrategy none = ReconnectStrategy.none();
        assertTrue(none.nextDelay(1, null).blockOptional().isEmpty());

        ReconnectStrategy fixed = ReconnectStrategy.fixedDelay(Duration.ofSeconds(5));
        assertEquals(Duration.ofSeconds(5), fixed.nextDelay(1, null).block());
        assertEquals(Duration.ofSeconds(5), fixed.nextDelay(10, null).block());

        ReconnectStrategy exponential = ReconnectStrategy.exponentialBackoff(
                Duration.ofSeconds(1), Duration.ofSeconds(30));
        assertEquals(Duration.ofSeconds(1), exponential.nextDelay(1, null).block());
        assertEquals(Duration.ofSeconds(2), exponential.nextDelay(2, null).block());
        assertEquals(Duration.ofSeconds(4), exponential.nextDelay(3, null).block());

        // 验证最大值限制
        Duration maxDelay = exponential.nextDelay(100, null).block();
        assertEquals(Duration.ofSeconds(30), maxDelay);

        // 带最大重试次数
        ReconnectStrategy limited = ReconnectStrategy.exponentialBackoff(
                Duration.ofSeconds(1), Duration.ofSeconds(30), 3);
        assertTrue(limited.nextDelay(1, null).blockOptional().isPresent());
        assertTrue(limited.nextDelay(3, null).blockOptional().isPresent());
        assertTrue(limited.nextDelay(4, null).blockOptional().isEmpty());
    }

    @Test
    void testClientIdGeneration() {
        MqttClientConnection conn = MqttClient.create()
                .host("127.0.0.1")
                .port(PORT)
                // 不设置 clientId，自动生成
                .connectNow(Duration.ofSeconds(5));

        assertNotNull(conn.getClientId());
        assertTrue(conn.getClientId().startsWith("reactor-mqtt-"));

        conn.disconnect().block(Duration.ofSeconds(5));
    }

    @Test
    void testProtocolVersion() {
        // MQTT 3.1.1
        MqttClientConnection conn311 = MqttClient.create()
                .host("127.0.0.1")
                .port(PORT)
                .clientId("test-mqtt311")
                .protocolVersion(MqttVersion.MQTT_3_1_1)
                .connectNow(Duration.ofSeconds(5));

        assertTrue(conn311.isConnected());
        conn311.disconnect().block(Duration.ofSeconds(5));

        // MQTT 5.0
        MqttClientConnection conn5 = MqttClient.create()
                .host("127.0.0.1")
                .port(PORT)
                .clientId("test-mqtt5")
                .protocolVersion(MqttVersion.MQTT_5)
                .connectNow(Duration.ofSeconds(5));

        assertTrue(conn5.isConnected());
        conn5.disconnect().block(Duration.ofSeconds(5));
    }

    @Test
    void testQos() {
        // 配置默认 QoS 为 AT_LEAST_ONCE
        MqttClientConnection conn = MqttClient.create()
                .host("127.0.0.1")
                .port(PORT)
                .clientId("test-default-qos")
                .qos(MqttQoS.AT_LEAST_ONCE)
                .connectNow(Duration.ofSeconds(5));

        assertEquals(MqttQoS.AT_LEAST_ONCE, conn.getQos());

        // 使用默认 QoS 发布消息（不指定 QoS）
        conn.publish("/test/topic",
                Unpooled.wrappedBuffer("Hello".getBytes(StandardCharsets.UTF_8)))
            .block(Duration.ofSeconds(5));

        conn.disconnect().block(Duration.ofSeconds(5));
    }
}
