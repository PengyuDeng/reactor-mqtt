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

import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.eclipse.paho.client.mqttv3.*;
import org.junit.jupiter.api.*;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MQTT Server 单元测试
 *
 * @author PengyuDeng
 */
class MqttServerTest {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 21883;

    private DisposableServer server;

    @AfterEach
    void tearDown() {
        if (server != null && !server.isDisposed()) {
            server.disposeNow();
        }
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
    void testClientConnect() throws Exception {
        AtomicReference<String> connectedClientId = new AtomicReference<>();
        CountDownLatch connectLatch = new CountDownLatch(1);

        server = MqttServer.create()
            .host(HOST)
            .port(PORT)
            .handle(connection -> {
                connectedClientId.set(connection.getClientId());
                connectLatch.countDown();
                return connection.listener(new NoOpListener()).accept();
            })
            .bindNow();

        MqttClient client = new MqttClient("tcp://" + HOST + ":" + PORT, "test-client-1");
        client.connect();

        assertTrue(connectLatch.await(5, TimeUnit.SECONDS));
        assertEquals("test-client-1", connectedClientId.get());

        client.disconnect();
    }

    @Test
    void testClientReject() throws Exception {
        server = MqttServer.create()
            .host(HOST)
            .port(PORT)
            .handle(connection -> connection.reject(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD))
            .bindNow();

        MqttClient client = new MqttClient("tcp://" + HOST + ":" + PORT, "test-client");

        assertThrows(MqttSecurityException.class, client::connect);
    }

    @Test
    void testAuthInfo() throws Exception {
        AtomicReference<MqttAuth> authRef = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        server = MqttServer.create()
            .host(HOST)
            .port(PORT)
            .handle(connection -> {
                authRef.set(connection.getAuth());
                latch.countDown();
                return connection.listener(new NoOpListener()).accept();
            })
            .bindNow();

        MqttClient client = new MqttClient("tcp://" + HOST + ":" + PORT, "test-client");
        MqttConnectOptions options = new MqttConnectOptions();
        options.setUserName("testuser");
        options.setPassword("testpass".toCharArray());
        client.connect(options);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals("testuser", authRef.get().getUsername());
        assertEquals("testpass", authRef.get().getPassword());

        client.disconnect();
    }

    // ==================== 消息发布测试 ====================

    @Test
    void testPublishQoS0() throws Exception {
        AtomicReference<String> receivedTopic = new AtomicReference<>();
        AtomicReference<String> receivedPayload = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        server = MqttServer.create()
            .host(HOST)
            .port(PORT)
            .handle(connection -> connection.listener(new MqttMessageListener() {
                @Override
                public Mono<Void> onPublish(MqttPublishing message) {
                    receivedTopic.set(message.getTopic());
                    receivedPayload.set(message.getPayload().toString(StandardCharsets.UTF_8));
                    latch.countDown();
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

        MqttClient client = new MqttClient("tcp://" + HOST + ":" + PORT, "test-client");
        client.connect();
        client.publish("test/topic", "hello".getBytes(), 0, false);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals("test/topic", receivedTopic.get());
        assertEquals("hello", receivedPayload.get());

        client.disconnect();
    }

    @Test
    void testPublishQoS1() throws Exception {
        AtomicInteger qosLevel = new AtomicInteger(-1);
        CountDownLatch latch = new CountDownLatch(1);

        server = MqttServer.create()
            .host(HOST)
            .port(PORT)
            .handle(connection -> connection.listener(new MqttMessageListener() {
                @Override
                public Mono<Void> onPublish(MqttPublishing message) {
                    qosLevel.set(message.getQosLevel());
                    latch.countDown();
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

        MqttClient client = new MqttClient("tcp://" + HOST + ":" + PORT, "test-client");
        client.connect();
        client.publish("test/topic", "hello".getBytes(), 1, false);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(1, qosLevel.get());

        client.disconnect();
    }

    @Test
    void testPublishQoS2() throws Exception {
        AtomicInteger qosLevel = new AtomicInteger(-1);
        CountDownLatch latch = new CountDownLatch(1);

        server = MqttServer.create()
            .host(HOST)
            .port(PORT)
            .handle(connection -> connection.listener(new MqttMessageListener() {
                @Override
                public Mono<Void> onPublish(MqttPublishing message) {
                    qosLevel.set(message.getQosLevel());
                    latch.countDown();
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

        MqttClient client = new MqttClient("tcp://" + HOST + ":" + PORT, "test-client");
        client.connect();
        client.publish("test/topic", "hello".getBytes(), 2, false);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(2, qosLevel.get());

        client.disconnect();
    }

    // ==================== 订阅测试 ====================

    @Test
    void testSubscribe() throws Exception {
        AtomicBoolean subscribed = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

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
                    subscribed.set(true);
                    latch.countDown();
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

        MqttClient client = new MqttClient("tcp://" + HOST + ":" + PORT, "test-client");
        client.connect();
        client.subscribe("test/#");

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertTrue(subscribed.get());

        client.disconnect();
    }

    @Test
    void testUnsubscribe() throws Exception {
        AtomicBoolean unsubscribed = new AtomicBoolean(false);
        CountDownLatch subscribeLatch = new CountDownLatch(1);
        CountDownLatch unsubscribeLatch = new CountDownLatch(1);

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
                    subscribeLatch.countDown();
                    return Mono.empty();
                }

                @Override
                public Mono<Void> onUnsubscribe(MqttUnSubscription unsubscription) {
                    unsubscribed.set(true);
                    unsubscribeLatch.countDown();
                    return Mono.empty();
                }

                @Override
                public Mono<Void> onDisconnect(MqttConnection connection) {
                    return Mono.empty();
                }
            }).accept())
            .bindNow();

        MqttClient client = new MqttClient("tcp://" + HOST + ":" + PORT, "test-client");
        client.connect();
        client.subscribe("test/#");
        assertTrue(subscribeLatch.await(5, TimeUnit.SECONDS));

        client.unsubscribe("test/#");
        assertTrue(unsubscribeLatch.await(5, TimeUnit.SECONDS));
        assertTrue(unsubscribed.get());

        client.disconnect();
    }

    // ==================== 断开连接测试 ====================

    @Test
    void testDisconnectCallback() throws Exception {
        AtomicBoolean disconnected = new AtomicBoolean(false);
        CountDownLatch connectLatch = new CountDownLatch(1);
        CountDownLatch disconnectLatch = new CountDownLatch(1);

        server = MqttServer.create()
            .host(HOST)
            .port(PORT)
            .handle(connection -> {
                connectLatch.countDown();
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
                        disconnected.set(true);
                        disconnectLatch.countDown();
                        return Mono.empty();
                    }
                }).accept();
            })
            .bindNow();

        MqttClient client = new MqttClient("tcp://" + HOST + ":" + PORT, "test-client");
        client.connect();
        assertTrue(connectLatch.await(5, TimeUnit.SECONDS));

        client.disconnect();
        assertTrue(disconnectLatch.await(5, TimeUnit.SECONDS));
        assertTrue(disconnected.get());
    }

    // ==================== 多消息测试 ====================

    @Test
    void testMultipleMessages() throws Exception {
        AtomicInteger messageCount = new AtomicInteger(0);
        int expectedCount = 100;
        CountDownLatch latch = new CountDownLatch(expectedCount);

        server = MqttServer.create()
            .host(HOST)
            .port(PORT)
            .handle(connection -> connection.listener(new MqttMessageListener() {
                @Override
                public Mono<Void> onPublish(MqttPublishing message) {
                    messageCount.incrementAndGet();
                    latch.countDown();
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

        MqttClient client = new MqttClient("tcp://" + HOST + ":" + PORT, "test-client");
        client.connect();

        for (int i = 0; i < expectedCount; i++) {
            client.publish("test/topic", ("msg-" + i).getBytes(), 1, false);
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertEquals(expectedCount, messageCount.get());

        client.disconnect();
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
