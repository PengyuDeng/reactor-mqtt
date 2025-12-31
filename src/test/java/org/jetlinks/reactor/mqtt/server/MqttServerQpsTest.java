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

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.mqtt.MqttClient;
import io.vertx.mqtt.MqttClientOptions;
import org.junit.jupiter.api.*;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * MQTT 服务器 QPS 测试
 *
 * 测试矩阵：
 * - 客户端数量：1 / 100 / 10000
 * - QoS 级别：0 / 1 / 2
 *
 * @author PengyuDeng
 */
class MqttServerQpsTest {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 11883;
    private static final int TEST_DURATION_SECONDS = 10;
    private static final Buffer PAYLOAD = Buffer.buffer("test");

    private DisposableServer server;
    private Vertx vertx;
    private final LongAdder receivedMessages = new LongAdder();

    @BeforeEach
    void setUp() {
        receivedMessages.reset();
        vertx = Vertx.vertx();

        server = MqttServer.create()
            .host(HOST)
            .port(PORT)
            .maxMessageSize(65536)
            .idleTimeout(Duration.ofSeconds(300))
            .handle(connection -> connection.listener(new MqttMessageListener() {
                @Override
                public Mono<Void> onPublish(MqttPublishing message) {
                    receivedMessages.increment();
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
            }).accept())
            .bindNow();

        System.out.println("MQTT 服务器已启动: tcp://" + HOST + ":" + PORT);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) {
            server.dispose();
            System.out.println("MQTT 服务器已停止\n");
        }
        if (vertx != null) {
            CountDownLatch latch = new CountDownLatch(1);
            vertx.close().onComplete(ar -> latch.countDown());
            latch.await(5, TimeUnit.SECONDS);
        }
    }

    // ==================== 1 客户端测试 ====================

    @Test
    void test_1_Client_QoS0() throws Exception {
        runQoSTest(1, MqttQoS.AT_MOST_ONCE);
    }

    @Test
    void test_1_Client_QoS1() throws Exception {
        runQoSTest(1, MqttQoS.AT_LEAST_ONCE);
    }

    @Test
    void test_1_Client_QoS2() throws Exception {
        runQoSTest(1, MqttQoS.EXACTLY_ONCE);
    }

    // ==================== 100 客户端测试 ====================

    @Test
    void test_100_Clients_QoS0() throws Exception {
        runQoSTest(100, MqttQoS.AT_MOST_ONCE);
    }

    @Test
    void test_100_Clients_QoS1() throws Exception {
        runQoSTest(100, MqttQoS.AT_LEAST_ONCE);
    }

    @Test
    void test_100_Clients_QoS2() throws Exception {
        runQoSTest(100, MqttQoS.EXACTLY_ONCE);
    }

    // ==================== 10000 客户端测试 ====================

    @Test
    void test_10000_Clients_QoS0() throws Exception {
        runQoSTest(10000, MqttQoS.AT_MOST_ONCE);
    }

    @Test
    void test_10000_Clients_QoS1() throws Exception {
        runQoSTest(10000, MqttQoS.AT_LEAST_ONCE);
    }

    @Test
    void test_10000_Clients_QoS2() throws Exception {
        runQoSTest(10000, MqttQoS.EXACTLY_ONCE);
    }

    /**
     * 统一的 QoS 测试方法
     */
    private void runQoSTest(int clientCount, MqttQoS qos) throws Exception {
        String qosName = "QoS " + qos.value();
        printHeader(qosName, clientCount);

        LongAdder deliveredCount = new LongAdder();
        LongAdder sentCount = new LongAdder();
        List<AtomicLong> inflightCounters = new ArrayList<>();

        // QoS 0 不需要背压控制，QoS 1/2 需要
        boolean needBackpressure = qos != MqttQoS.AT_MOST_ONCE;
        int maxInflight = clientCount == 1 ? 5000 : (clientCount <= 100 ? 1000 : 100);

        List<MqttClient> clients = new ArrayList<>();
        CountDownLatch connectLatch = new CountDownLatch(clientCount);

        for (int i = 0; i < clientCount; i++) {
            AtomicLong inflightCount = new AtomicLong(0);
            inflightCounters.add(inflightCount);

            MqttClientOptions options = new MqttClientOptions()
                .setClientId("client-" + qos.value() + "-" + i)
                .setCleanSession(true)
                .setKeepAliveInterval(300);

            if (needBackpressure) {
                options.setMaxInflightQueue(maxInflight);
            }

            MqttClient client = MqttClient.create(vertx, options);

            if (needBackpressure) {
                client.publishCompletionHandler(id -> {
                    deliveredCount.increment();
                    inflightCount.decrementAndGet();
                });
            }

            client.connect(PORT, HOST).onComplete(ar -> {
                if (ar.succeeded()) {
                    synchronized (clients) {
                        clients.add(client);
                    }
                }
                connectLatch.countDown();
            });
        }

        boolean connected = connectLatch.await(Math.max(60, clientCount / 100), TimeUnit.SECONDS);
        if (!connected) {
            System.out.println("警告：部分客户端连接超时，已连接: " + clients.size());
        }
        System.out.println("已连接客户端: " + clients.size() + "，开始测试...\n");

        Thread.sleep(500);

        AtomicLong startTime = new AtomicLong();
        int actualClients = clients.size();

        ExecutorService executor = Executors.newFixedThreadPool(Math.min(actualClients, 200));
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(actualClients);

        ScheduledExecutorService statsExecutor = needBackpressure
            ? startQoS12StatsReporter(deliveredCount, startTime)
            : startQoS0StatsReporter(sentCount, startTime);

        for (int i = 0; i < actualClients; i++) {
            MqttClient client = clients.get(i);
            AtomicLong inflightCount = inflightCounters.get(i);
            final int finalMaxInflight = maxInflight;

            executor.submit(() -> {
                try {
                    startLatch.await();
                    long endTime = startTime.get() + (TEST_DURATION_SECONDS * 1000L);
                    String topic = "test/qos" + qos.value();

                    while (System.currentTimeMillis() < endTime) {
                        // QoS 1/2 背压控制
                        if (needBackpressure && inflightCount.get() >= finalMaxInflight - 10) {
                            Thread.sleep(1);
                            continue;
                        }

                        try {
                            client.publish(topic, PAYLOAD, qos, false, false);
                            sentCount.increment();
                            if (needBackpressure) {
                                inflightCount.incrementAndGet();
                            }
                        } catch (Exception e) {
                            // ignore
                        }
                    }
                } catch (Exception e) {
                    // ignore
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startTime.set(System.currentTimeMillis());
        startLatch.countDown();

        endLatch.await(TEST_DURATION_SECONDS + 30, TimeUnit.SECONDS);
        statsExecutor.shutdown();
        Thread.sleep(needBackpressure ? 3000 : 2000);

        long totalTime = System.currentTimeMillis() - startTime.get();

        if (needBackpressure) {
            printQoS12Result(qosName, actualClients, sentCount.sum(), deliveredCount.sum(),
                             receivedMessages.sum(), totalTime);
        } else {
            printQoS0Result(actualClients, sentCount.sum(), receivedMessages.sum(), totalTime);
        }

        disconnectClients(clients);
        executor.shutdown();
    }

    /**
     * 连接客户端
     */
    private List<MqttClient> connectClients(int count) throws Exception {
        List<MqttClient> clients = new ArrayList<>();
        CountDownLatch connectLatch = new CountDownLatch(count);

        for (int i = 0; i < count; i++) {
            MqttClient client = MqttClient.create(vertx, new MqttClientOptions()
                .setClientId("client-" + i)
                .setCleanSession(true)
                .setKeepAliveInterval(300));

            client.connect(PORT, HOST).onComplete(ar -> {
                if (ar.succeeded()) {
                    synchronized (clients) {
                        clients.add(client);
                    }
                }
                connectLatch.countDown();
            });
        }

        boolean connected = connectLatch.await(Math.max(60, count / 100), TimeUnit.SECONDS);
        if (!connected) {
            System.out.println("警告：部分客户端连接超时，已连接: " + clients.size());
        }

        return clients;
    }

    /**
     * 断开客户端
     */
    private void disconnectClients(List<MqttClient> clients) throws Exception {
        CountDownLatch disconnectLatch = new CountDownLatch(clients.size());
        for (MqttClient client : clients) {
            try {
                client.disconnect().onComplete(ar -> disconnectLatch.countDown());
            } catch (Exception e) {
                disconnectLatch.countDown();
            }
        }
        disconnectLatch.await(30, TimeUnit.SECONDS);
    }

    /**
     * QoS 0 实时统计
     */
    private ScheduledExecutorService startQoS0StatsReporter(LongAdder sentCount, AtomicLong startTime) {
        ScheduledExecutorService statsExecutor = Executors.newSingleThreadScheduledExecutor();
        AtomicLong lastSent = new AtomicLong(0);
        AtomicLong lastRecv = new AtomicLong(0);
        AtomicLong lastTime = new AtomicLong(0);

        statsExecutor.scheduleAtFixedRate(() -> {
            long currentSent = sentCount.sum();
            long currentRecv = receivedMessages.sum();
            long currentTime = System.currentTimeMillis();

            if (lastTime.get() > 0) {
                long timeDiff = currentTime - lastTime.get();
                if (timeDiff > 0) {
                    long sendQps = (currentSent - lastSent.get()) * 1000 / timeDiff;
                    long recvQps = (currentRecv - lastRecv.get()) * 1000 / timeDiff;
                    System.out.printf("  发送: %,d/s | 接收: %,d/s | 总发送: %,d | 总接收: %,d%n",
                        sendQps, recvQps, currentSent, currentRecv);
                }
            }

            lastSent.set(currentSent);
            lastRecv.set(currentRecv);
            lastTime.set(currentTime);
        }, 1, 1, TimeUnit.SECONDS);

        return statsExecutor;
    }

    /**
     * QoS 1/2 实时统计
     */
    private ScheduledExecutorService startQoS12StatsReporter(LongAdder deliveredCount, AtomicLong startTime) {
        ScheduledExecutorService statsExecutor = Executors.newSingleThreadScheduledExecutor();
        AtomicLong lastDelivered = new AtomicLong(0);
        AtomicLong lastRecv = new AtomicLong(0);
        AtomicLong lastTime = new AtomicLong(0);

        statsExecutor.scheduleAtFixedRate(() -> {
            long currentDelivered = deliveredCount.sum();
            long currentRecv = receivedMessages.sum();
            long currentTime = System.currentTimeMillis();

            if (lastTime.get() > 0) {
                long timeDiff = currentTime - lastTime.get();
                if (timeDiff > 0) {
                    long deliveredQps = (currentDelivered - lastDelivered.get()) * 1000 / timeDiff;
                    long recvQps = (currentRecv - lastRecv.get()) * 1000 / timeDiff;
                    System.out.printf("  确认: %,d/s | 接收: %,d/s | 总确认: %,d | 总接收: %,d%n",
                        deliveredQps, recvQps, currentDelivered, currentRecv);
                }
            }

            lastDelivered.set(currentDelivered);
            lastRecv.set(currentRecv);
            lastTime.set(currentTime);
        }, 1, 1, TimeUnit.SECONDS);

        return statsExecutor;
    }

    private void printHeader(String qos, int clientCount) {
        System.out.println("\n========================================");
        System.out.printf("  %s 测试 - %,d 客户端%n", qos, clientCount);
        System.out.println("========================================");
        System.out.println("测试时长: " + TEST_DURATION_SECONDS + " 秒");
        System.out.println("消息大小: " + PAYLOAD.length() + " 字节");
        System.out.println("----------------------------------------");
    }

    private void printQoS0Result(int clientCount, long sent, long received, long totalTime) {
        double sendQps = sent * 1000.0 / totalTime;
        double recvQps = received * 1000.0 / totalTime;
        double lossRate = sent > 0 ? (1 - (double) received / sent) * 100 : 0;

        System.out.println("\n========================================");
        System.out.printf("  结果 - QoS 0 - %,d 客户端%n", clientCount);
        System.out.println("========================================");
        System.out.printf("总发送: %,d | 总接收: %,d%n", sent, received);
        System.out.printf("测试时长: %.2f 秒%n", totalTime / 1000.0);
        System.out.println("----------------------------------------");
        System.out.printf("发送 QPS: %,.0f/s%n", sendQps);
        System.out.printf("接收 QPS: %,.0f/s%n", recvQps);
        System.out.printf("丢失率: %.4f%%%n", lossRate);
        System.out.println("========================================");
    }

    private void printQoS12Result(String qosName, int clientCount, long sent, long delivered, long received, long totalTime) {
        double deliveredQps = delivered * 1000.0 / totalTime;
        double recvQps = received * 1000.0 / totalTime;

        System.out.println("\n========================================");
        System.out.printf("  结果 - %s - %,d 客户端%n", qosName, clientCount);
        System.out.println("========================================");
        System.out.printf("总发送: %,d | 总确认: %,d | 服务器接收: %,d%n", sent, delivered, received);
        System.out.printf("测试时长: %.2f 秒%n", totalTime / 1000.0);
        System.out.println("----------------------------------------");
        System.out.printf("确认 QPS: %,.0f/s（端到端吞吐量）%n", deliveredQps);
        System.out.printf("接收 QPS: %,.0f/s%n", recvQps);
        System.out.println("========================================");
    }
}
