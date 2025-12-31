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
package server;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.mqtt.MqttClient;
import io.vertx.mqtt.MqttClientOptions;
import org.jetlinks.reactor.mqtt.server.*;
import org.junit.jupiter.api.*;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MQTT 服务器压力测试 - 使用 Vert.x 客户端
 */
class MqttServerStressTest {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 11883;

    private DisposableServer server;
    private Vertx vertx;
    private final AtomicInteger receivedMessages = new AtomicInteger(0);
    private final AtomicInteger connectedClients = new AtomicInteger(0);

    @BeforeEach
    void setUp() {
        receivedMessages.set(0);
        connectedClients.set(0);

        vertx = Vertx.vertx();

        server = MqttServer.create()
            .host(HOST)
            .port(PORT)
            .maxMessageSize(65536)
            .idleTimeout(Duration.ofSeconds(60))
            .handle(connection -> {
                connectedClients.incrementAndGet();

                connection.onDispose()
                    .doOnSuccess(v -> connectedClients.decrementAndGet())
                    .subscribe();

                return connection.listener(new MqttMessageListener() {
                    @Override
                    public Mono<Void> onPublish(MqttPublishing message) {
                        receivedMessages.incrementAndGet();
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
                        return Mono.empty();
                    }
                }).accept();
            })
            .bindNow();

        System.out.println("MQTT 服务器已启动: tcp://" + HOST + ":" + PORT);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) {
            server.dispose();
            System.out.println("MQTT 服务器已停止");
        }
        if (vertx != null) {
            CountDownLatch latch = new CountDownLatch(1);
            vertx.close().onComplete(ar -> latch.countDown());
            latch.await(5, TimeUnit.SECONDS);
        }
    }

    /**
     * 测试并发连接
     */
    @Test
    void testConcurrentConnections() throws Exception {
        int clientCount = 100;
        CountDownLatch connectLatch = new CountDownLatch(clientCount);
        List<MqttClient> clients = new CopyOnWriteArrayList<>();
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < clientCount; i++) {
            MqttClient client = MqttClient.create(vertx, new MqttClientOptions()
                .setClientId("stress-conn-" + i)
                .setCleanSession(true)
                .setKeepAliveInterval(30));

            client.connect(PORT, HOST)
                .onSuccess(ack -> {
                    clients.add(client);
                    successCount.incrementAndGet();
                    connectLatch.countDown();
                })
                .onFailure(err -> {
                    failCount.incrementAndGet();
                    System.err.println("连接失败: " + err.getMessage());
                    connectLatch.countDown();
                });
        }

        assertTrue(connectLatch.await(30, TimeUnit.SECONDS), "连接超时");
        long elapsed = System.currentTimeMillis() - startTime;

        System.out.println("=== 并发连接测试结果 ===");
        System.out.println("总客户端数: " + clientCount);
        System.out.println("成功连接: " + successCount.get());
        System.out.println("连接失败: " + failCount.get());
        System.out.println("耗时: " + elapsed + " ms");
        System.out.println("连接速率: " + (clientCount * 1000.0 / elapsed) + " 连接/秒");

        assertEquals(clientCount, successCount.get());
        assertEquals(clientCount, connectedClients.get());

        // 断开所有客户端
        CountDownLatch disconnectLatch = new CountDownLatch(clients.size());
        for (MqttClient client : clients) {
            client.disconnect().onComplete(ar -> disconnectLatch.countDown());
        }
        disconnectLatch.await(10, TimeUnit.SECONDS);
    }

    /**
     * 测试高吞吐量消息发布
     */
    @Test
    void testHighThroughputPublish() throws Exception {
        int messageCount = 10000;
        String topic = "stress/throughput";
        Buffer payload = Buffer.buffer("Hello MQTT Stress Test");

        CountDownLatch connectLatch = new CountDownLatch(1);
        MqttClient client = MqttClient.create(vertx, new MqttClientOptions()
            .setClientId("stress-throughput")
            .setCleanSession(true));

        client.connect(PORT, HOST).onComplete(ar -> connectLatch.countDown());
        assertTrue(connectLatch.await(5, TimeUnit.SECONDS));

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < messageCount; i++) {
            client.publish(topic, payload, MqttQoS.AT_MOST_ONCE, false, false);
        }

        Thread.sleep(2000);
        long elapsed = System.currentTimeMillis() - startTime;

        System.out.println("=== 高吞吐量测试结果 ===");
        System.out.println("发送消息数: " + messageCount);
        System.out.println("接收消息数: " + receivedMessages.get());
        System.out.println("耗时: " + elapsed + " ms");
        System.out.println("吞吐量: " + (messageCount * 1000.0 / elapsed) + " 消息/秒");

        assertTrue(receivedMessages.get() >= messageCount * 0.95, "至少应接收95%的消息");

        CountDownLatch disconnectLatch = new CountDownLatch(1);
        client.disconnect().onComplete(ar -> disconnectLatch.countDown());
        disconnectLatch.await(5, TimeUnit.SECONDS);
    }

    /**
     * 测试多客户端并发发布
     */
    @Test
    void testConcurrentPublish() throws Exception {
        int clientCount = 10;
        int messagesPerClient = 1000;
        int totalMessages = clientCount * messagesPerClient;
        String topic = "stress/concurrent";
        Buffer payload = Buffer.buffer("Concurrent message");

        List<MqttClient> clients = new ArrayList<>();
        CountDownLatch connectLatch = new CountDownLatch(clientCount);

        for (int i = 0; i < clientCount; i++) {
            MqttClient client = MqttClient.create(vertx, new MqttClientOptions()
                .setClientId("stress-pub-" + i)
                .setCleanSession(true));
            client.connect(PORT, HOST).onComplete(ar -> {
                if (ar.succeeded()) {
                    clients.add(client);
                }
                connectLatch.countDown();
            });
        }
        assertTrue(connectLatch.await(10, TimeUnit.SECONDS));

        CountDownLatch publishLatch = new CountDownLatch(clientCount);
        long startTime = System.currentTimeMillis();

        for (MqttClient client : clients) {
            vertx.executeBlocking(() -> {
                for (int j = 0; j < messagesPerClient; j++) {
                    client.publish(topic, payload, MqttQoS.AT_MOST_ONCE, false, false);
                }
                return null;
            }).onComplete(ar -> publishLatch.countDown());
        }

        assertTrue(publishLatch.await(60, TimeUnit.SECONDS), "发布超时");
        Thread.sleep(2000);

        long elapsed = System.currentTimeMillis() - startTime;

        System.out.println("=== 并发发布测试结果 ===");
        System.out.println("客户端数: " + clientCount);
        System.out.println("每客户端消息数: " + messagesPerClient);
        System.out.println("总消息数: " + totalMessages);
        System.out.println("接收消息数: " + receivedMessages.get());
        System.out.println("耗时: " + elapsed + " ms");
        System.out.println("吞吐量: " + (totalMessages * 1000.0 / elapsed) + " 消息/秒");

        assertTrue(receivedMessages.get() >= totalMessages * 0.95);

        CountDownLatch disconnectLatch = new CountDownLatch(clients.size());
        for (MqttClient client : clients) {
            client.disconnect().onComplete(ar -> disconnectLatch.countDown());
        }
        disconnectLatch.await(10, TimeUnit.SECONDS);
    }

    /**
     * 测试 QoS 1 消息确认
     */
    @Test
    void testQoS1Acknowledgment() throws Exception {
        int messageCount = 1000;
        String topic = "stress/qos1";
        Buffer payload = Buffer.buffer("QoS 1 message");
        AtomicInteger deliveredCount = new AtomicInteger(0);

        CountDownLatch connectLatch = new CountDownLatch(1);
        MqttClient client = MqttClient.create(vertx, new MqttClientOptions()
            .setClientId("stress-qos1")
            .setCleanSession(true)
            .setMaxInflightQueue(1000));

        client.publishCompletionHandler(id -> deliveredCount.incrementAndGet());

        client.connect(PORT, HOST).onComplete(ar -> connectLatch.countDown());
        assertTrue(connectLatch.await(5, TimeUnit.SECONDS));

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < messageCount; i++) {
            client.publish(topic, payload, MqttQoS.AT_LEAST_ONCE, false, false);
        }

        // 等待 ACK
        int maxWait = 50;
        while (deliveredCount.get() < messageCount && maxWait-- > 0) {
            Thread.sleep(100);
        }

        long elapsed = System.currentTimeMillis() - startTime;

        System.out.println("=== QoS 1 测试结果 ===");
        System.out.println("发送消息数: " + messageCount);
        System.out.println("确认送达数: " + deliveredCount.get());
        System.out.println("服务器接收数: " + receivedMessages.get());
        System.out.println("耗时: " + elapsed + " ms");

        assertEquals(messageCount, deliveredCount.get(), "所有消息应确认送达");
        assertEquals(messageCount, receivedMessages.get(), "所有消息应被接收");

        CountDownLatch disconnectLatch = new CountDownLatch(1);
        client.disconnect().onComplete(ar -> disconnectLatch.countDown());
        disconnectLatch.await(5, TimeUnit.SECONDS);
    }

    /**
     * 测试持续压力
     */
    @Test
    void testSustainedLoad() throws Exception {
        int durationSeconds = 10;
        int clientCount = 5;
        String topic = "stress/sustained";
        Buffer payload = Buffer.buffer("Sustained load message");
        AtomicLong sentCount = new AtomicLong(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        List<MqttClient> clients = new ArrayList<>();
        CountDownLatch connectLatch = new CountDownLatch(clientCount);

        for (int i = 0; i < clientCount; i++) {
            MqttClient client = MqttClient.create(vertx, new MqttClientOptions()
                .setClientId("stress-sustained-" + i)
                .setCleanSession(true));
            client.connect(PORT, HOST).onComplete(ar -> {
                if (ar.succeeded()) {
                    clients.add(client);
                }
                connectLatch.countDown();
            });
        }
        assertTrue(connectLatch.await(10, TimeUnit.SECONDS));

        ExecutorService executor = Executors.newFixedThreadPool(clientCount);
        long endTime = System.currentTimeMillis() + (durationSeconds * 1000L);

        for (MqttClient client : clients) {
            executor.submit(() -> {
                while (System.currentTimeMillis() < endTime) {
                    try {
                        client.publish(topic, payload, MqttQoS.AT_MOST_ONCE, false, false);
                        sentCount.incrementAndGet();
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                    }
                }
            });
        }

        Thread.sleep(durationSeconds * 1000L + 2000);
        executor.shutdown();

        System.out.println("=== 持续压力测试结果 ===");
        System.out.println("持续时间: " + durationSeconds + " 秒");
        System.out.println("客户端数: " + clientCount);
        System.out.println("发送消息数: " + sentCount.get());
        System.out.println("错误数: " + errorCount.get());
        System.out.println("服务器接收数: " + receivedMessages.get());
        System.out.println("平均吞吐量: " + (sentCount.get() / durationSeconds) + " 消息/秒");

        assertTrue(errorCount.get() < sentCount.get() * 0.01, "错误率应低于1%");

        CountDownLatch disconnectLatch = new CountDownLatch(clients.size());
        for (MqttClient client : clients) {
            client.disconnect().onComplete(ar -> disconnectLatch.countDown());
        }
        disconnectLatch.await(10, TimeUnit.SECONDS);
    }

    /**
     * 测试大消息
     */
    @Test
    void testLargeMessages() throws Exception {
        int messageCount = 100;
        int messageSize = 32 * 1024;
        String topic = "stress/large";
        byte[] payloadBytes = new byte[messageSize];
        for (int i = 0; i < messageSize; i++) {
            payloadBytes[i] = (byte) (i % 256);
        }
        Buffer payload = Buffer.buffer(payloadBytes);
        AtomicInteger deliveredCount = new AtomicInteger(0);

        CountDownLatch connectLatch = new CountDownLatch(1);
        MqttClient client = MqttClient.create(vertx, new MqttClientOptions()
            .setClientId("stress-large")
            .setCleanSession(true)
            .setMaxInflightQueue(1000));

        client.publishCompletionHandler(id -> deliveredCount.incrementAndGet());

        client.connect(PORT, HOST).onComplete(ar -> connectLatch.countDown());
        assertTrue(connectLatch.await(5, TimeUnit.SECONDS));

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < messageCount; i++) {
            client.publish(topic, payload, MqttQoS.AT_LEAST_ONCE, false, false);
        }

        // 等待 ACK
        int maxWait = 50;
        while (deliveredCount.get() < messageCount && maxWait-- > 0) {
            Thread.sleep(100);
        }

        long elapsed = System.currentTimeMillis() - startTime;

        double dataMB = (messageCount * messageSize) / (1024.0 * 1024.0);

        System.out.println("=== 大消息测试结果 ===");
        System.out.println("消息数: " + messageCount);
        System.out.println("消息大小: " + (messageSize / 1024) + " KB");
        System.out.println("总数据量: " + String.format("%.2f", dataMB) + " MB");
        System.out.println("耗时: " + elapsed + " ms");
        System.out.println("吞吐量: " + String.format("%.2f", dataMB * 1000 / elapsed) + " MB/秒");
        System.out.println("接收消息数: " + receivedMessages.get());

        assertEquals(messageCount, receivedMessages.get());

        CountDownLatch disconnectLatch = new CountDownLatch(1);
        client.disconnect().onComplete(ar -> disconnectLatch.countDown());
        disconnectLatch.await(5, TimeUnit.SECONDS);
    }

    /**
     * 测试快速连接断开
     */
    @Test
    void testRapidConnectDisconnect() throws Exception {
        int iterations = 50;
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < iterations; i++) {
            CountDownLatch latch = new CountDownLatch(1);
            MqttClient client = MqttClient.create(vertx, new MqttClientOptions()
                .setClientId("stress-rapid-" + i)
                .setCleanSession(true));

            final int iter = i;
            client.connect(PORT, HOST)
                .compose(ack -> client.disconnect())
                .onSuccess(v -> {
                    successCount.incrementAndGet();
                    latch.countDown();
                })
                .onFailure(err -> {
                    failCount.incrementAndGet();
                    System.err.println("第 " + iter + " 次迭代失败: " + err.getMessage());
                    latch.countDown();
                });

            latch.await(5, TimeUnit.SECONDS);
        }

        long elapsed = System.currentTimeMillis() - startTime;

        System.out.println("=== 快速连接/断开测试结果 ===");
        System.out.println("迭代次数: " + iterations);
        System.out.println("成功: " + successCount.get());
        System.out.println("失败: " + failCount.get());
        System.out.println("耗时: " + elapsed + " ms");
        System.out.println("速率: " + (iterations * 1000.0 / elapsed) + " 次/秒");

        assertEquals(iterations, successCount.get());
    }
}
