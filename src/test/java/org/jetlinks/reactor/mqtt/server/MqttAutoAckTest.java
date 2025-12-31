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
package org.jetlinks.reactor.mqtt.server;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 测试 autoAck 功能
 *
 * @author PengyuDeng
 */
class MqttAutoAckTest {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 21885;  // 使用不同端口避免冲突

    private DisposableServer server;

    @AfterEach
    void tearDown() {
        if (server != null && !server.isDisposed()) {
            server.disposeNow();
        }
    }

    /**
     * 创建 MQTT 客户端
     *
     * @param clientId 客户端 ID
     * @return 已连接的 ReactorMqttClient 实例
     */
    private ReactorMqttClient createClient(String clientId) {
        ReactorMqttClient client = new ReactorMqttClient(HOST, PORT, clientId);
        client.connect();
        return client;
    }

    /**
     * 测试自动应答模式（默认）
     * QoS1 消息处理完成后自动发送 PUBACK
     */
    @Test
    void testAutoAckDefault() throws Exception {
        AtomicReference<String> receivedPayload = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        server = MqttServer.create()
                           .host(HOST)
                           .port(PORT)
                           .handle(connection -> connection
                                   // 默认 autoAck = true
                                   .listener(new MqttMessageListener() {
                                       @Override
                                       public Mono<Void> onPublish(MqttPublishing message) {
                                           receivedPayload.set(message.getPayload().toString(StandardCharsets.UTF_8));
                                           latch.countDown();
                                           // 不调用 acknowledge()，依赖自动应答
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
                                   }).accept())
                           .bindNow();

        ReactorMqttClient client = createClient("auto-ack-client");

        // 发送 QoS1 消息，如果自动应答正常，客户端不会超时
        client.publish("test/auto-ack", "auto-ack-message".getBytes(), 1, false);

        assertTrue(latch.await(5, TimeUnit.SECONDS), "消息应该被接收");
        assertEquals("auto-ack-message", receivedPayload.get());

        client.disconnect();
    }

    /**
     * 测试显式开启自动应答
     */
    @Test
    void testAutoAckExplicitTrue() throws Exception {
        AtomicInteger messageCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(3);

        server = MqttServer.create()
                           .host(HOST)
                           .port(PORT)
                           .handle(connection -> connection
                                   .autoAck(true)  // 显式设置自动应答
                                   .listener(new MqttMessageListener() {
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
                                       public Mono<Void> onUnsubscribe(MqttUnsubscription unsubscription) {
                                           return Mono.empty();
                                       }

                                       @Override
                                       public Mono<Void> onDisconnect(MqttConnection connection) {
                                           return Mono.empty();
                                       }
                                   }).accept())
                           .bindNow();

        ReactorMqttClient client = createClient("auto-ack-explicit-client");

        // 发送多条 QoS1 消息
        for (int i = 0; i < 3; i++) {
            client.publish("test/auto-ack", ("message-" + i).getBytes(), 1, false);
        }

        assertTrue(latch.await(5, TimeUnit.SECONDS), "所有消息应该被接收");
        assertEquals(3, messageCount.get());

        client.disconnect();
    }

    /**
     * 测试手动应答模式
     * 处理者需要自己调用 acknowledge()
     */
    @Test
    void testManualAck() throws Exception {
        AtomicReference<String> receivedPayload = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        server = MqttServer.create()
                           .host(HOST)
                           .port(PORT)
                           .handle(connection -> connection
                                   .autoAck(false)  // 手动应答模式
                                   .listener(new MqttMessageListener() {
                                       @Override
                                       public Mono<Void> onPublish(MqttPublishing message) {
                                           receivedPayload.set(message.getPayload().toString(StandardCharsets.UTF_8));
                                           latch.countDown();
                                           // 手动应答：模拟处理后再确认
                                           return Mono.delay(java.time.Duration.ofMillis(100))
                                                      .then(message.acknowledge());
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
                                   }).accept())
                           .bindNow();

        ReactorMqttClient client = createClient("manual-ack-client");

        // 发送 QoS1 消息
        client.publish("test/manual-ack", "manual-ack-message".getBytes(), 1, false);

        assertTrue(latch.await(5, TimeUnit.SECONDS), "消息应该被接收");
        assertEquals("manual-ack-message", receivedPayload.get());

        client.disconnect();
    }

    /**
     * 测试手动应答模式下的 QoS2 消息
     */
    @Test
    void testManualAckQoS2() throws Exception {
        AtomicReference<String> receivedPayload = new AtomicReference<>();
        AtomicInteger qosLevel = new AtomicInteger(-1);
        CountDownLatch latch = new CountDownLatch(1);

        server = MqttServer.create()
                           .host(HOST)
                           .port(PORT)
                           .handle(connection -> connection
                                   .autoAck(false)
                                   .listener(new MqttMessageListener() {
                                       @Override
                                       public Mono<Void> onPublish(MqttPublishing message) {
                                           receivedPayload.set(message.getPayload().toString(StandardCharsets.UTF_8));
                                           qosLevel.set(message.getQosLevel());
                                           latch.countDown();
                                           // 手动应答 QoS2
                                           return message.acknowledge();
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
                                   }).accept())
                           .bindNow();

        ReactorMqttClient client = createClient("manual-ack-qos2-client");

        // 发送 QoS2 消息
        client.publish("test/manual-ack-qos2", "qos2-message".getBytes(), 2, false);

        assertTrue(latch.await(5, TimeUnit.SECONDS), "消息应该被接收");
        assertEquals("qos2-message", receivedPayload.get());
        assertEquals(2, qosLevel.get());

        client.disconnect();
    }

    /**
     * 测试 QoS0 消息不受 autoAck 影响（QoS0 无需应答）
     */
    @Test
    void testQoS0NotAffectedByAutoAck() throws Exception {
        AtomicInteger messageCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(2);

        server = MqttServer.create()
                           .host(HOST)
                           .port(PORT)
                           .handle(connection -> connection
                                   .autoAck(false)  // 即使设置手动应答，QoS0 也不需要
                                   .listener(new MqttMessageListener() {
                                       @Override
                                       public Mono<Void> onPublish(MqttPublishing message) {
                                           messageCount.incrementAndGet();
                                           latch.countDown();
                                           // QoS0 不需要调用 acknowledge()
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
                                   }).accept())
                           .bindNow();

        ReactorMqttClient client = createClient("qos0-client");

        // 发送 QoS0 消息
        client.publish("test/qos0", "message1".getBytes(), 0, false);
        client.publish("test/qos0", "message2".getBytes(), 0, false);

        assertTrue(latch.await(5, TimeUnit.SECONDS), "所有消息应该被接收");
        assertEquals(2, messageCount.get());

        client.disconnect();
    }

    /**
     * 测试手动应答模式下多条消息的顺序处理
     */
    @Test
    void testManualAckMultipleMessages() throws Exception {
        AtomicInteger messageCount = new AtomicInteger(0);
        int expectedCount = 10;
        CountDownLatch latch = new CountDownLatch(expectedCount);

        server = MqttServer.create()
                           .host(HOST)
                           .port(PORT)
                           .handle(connection -> connection
                                   .autoAck(false)
                                   .listener(new MqttMessageListener() {
                                       @Override
                                       public Mono<Void> onPublish(MqttPublishing message) {
                                           messageCount.incrementAndGet();
                                           latch.countDown();
                                           // 每条消息都手动应答
                                           return message.acknowledge();
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
                                   }).accept())
                           .bindNow();

        ReactorMqttClient client = createClient("multi-msg-client");

        // 发送多条 QoS1 消息
        for (int i = 0; i < expectedCount; i++) {
            client.publish("test/multi", ("msg-" + i).getBytes(), 1, false);
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS), "所有消息应该被接收");
        assertEquals(expectedCount, messageCount.get());

        client.disconnect();
    }
}
