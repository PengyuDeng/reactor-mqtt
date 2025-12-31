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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * MQTT 客户端发送测试 - 独立运行，用于跨机器测试
 *
 * 使用方式：
 * 1. 修改 SERVER_HOST 为目标服务器 IP
 * 2. 运行 main 方法
 *
 * @author PengyuDeng
 */
public class MqttClientSendTest {

    // ================= 配置参数 =================
    private static final String SERVER_HOST = "127.0.0.1";  // 服务器地址，修改为目标机器 IP
    private static final int SERVER_PORT = 11883;           // 服务器端口
    private static final int CLIENT_COUNT = 1;              // 客户端数量
    private static final int TEST_DURATION_SECONDS = 10;    // 测试时长（秒）
    private static final MqttQoS QOS = MqttQoS.AT_MOST_ONCE; // QoS 级别
    private static final String TOPIC = "test/benchmark";   // 发送主题
    private static final Buffer PAYLOAD = Buffer.buffer("test-payload"); // 消息内容
    // =============================================

    public static void main(String[] args) throws Exception {
        System.out.println("========================================");
        System.out.println("  MQTT 客户端发送测试");
        System.out.println("========================================");
        System.out.println("服务器: " + SERVER_HOST + ":" + SERVER_PORT);
        System.out.println("客户端数量: " + CLIENT_COUNT);
        System.out.println("QoS: " + QOS.value());
        System.out.println("测试时长: " + TEST_DURATION_SECONDS + " 秒");
        System.out.println("消息大小: " + PAYLOAD.length() + " 字节");
        System.out.println("----------------------------------------\n");

        Vertx vertx = Vertx.vertx();
        LongAdder sentCount = new LongAdder();
        LongAdder deliveredCount = new LongAdder();
        List<MqttClient> clients = new ArrayList<>();

        try {
            // 连接客户端
            System.out.println("正在连接客户端...");
            CountDownLatch connectLatch = new CountDownLatch(CLIENT_COUNT);

            for (int i = 0; i < CLIENT_COUNT; i++) {
                MqttClientOptions options = new MqttClientOptions()
                    .setClientId("bench-client-" + i)
                    .setCleanSession(true)
                    .setKeepAliveInterval(300);

                if (QOS != MqttQoS.AT_MOST_ONCE) {
                    options.setMaxInflightQueue(10000);
                }

                MqttClient client = MqttClient.create(vertx, options);

                if (QOS != MqttQoS.AT_MOST_ONCE) {
                    client.publishCompletionHandler(id -> deliveredCount.increment());
                }

                client.connect(SERVER_PORT, SERVER_HOST).onComplete(ar -> {
                    if (ar.succeeded()) {
                        synchronized (clients) {
                            clients.add(client);
                        }
                    } else {
                        System.out.println("连接失败: " + ar.cause().getMessage());
                    }
                    connectLatch.countDown();
                });
            }

            boolean connected = connectLatch.await(60, TimeUnit.SECONDS);
            if (!connected) {
                System.out.println("警告：部分客户端连接超时");
            }
            System.out.println("已连接客户端: " + clients.size());

            if (clients.isEmpty()) {
                System.out.println("错误：没有客户端连接成功");
                return;
            }

            Thread.sleep(500);

            // 启动发送
            System.out.println("\n开始发送消息...\n");

            ExecutorService executor = Executors.newFixedThreadPool(Math.min(clients.size(), 200));
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch endLatch = new CountDownLatch(clients.size());
            AtomicLong startTime = new AtomicLong();

            // 实时统计线程
            ScheduledExecutorService statsExecutor = Executors.newSingleThreadScheduledExecutor();
            AtomicLong lastSent = new AtomicLong(0);
            AtomicLong lastTime = new AtomicLong(0);

            statsExecutor.scheduleAtFixedRate(() -> {
                long currentSent = sentCount.sum();
                long currentTime = System.currentTimeMillis();

                if (lastTime.get() > 0) {
                    long timeDiff = currentTime - lastTime.get();
                    if (timeDiff > 0) {
                        long sendQps = (currentSent - lastSent.get()) * 1000 / timeDiff;
                        if (QOS == MqttQoS.AT_MOST_ONCE) {
                            System.out.printf("  发送: %,d/s | 总发送: %,d%n", sendQps, currentSent);
                        } else {
                            long currentDelivered = deliveredCount.sum();
                            System.out.printf("  发送: %,d/s | 总发送: %,d | 已确认: %,d%n",
                                sendQps, currentSent, currentDelivered);
                        }
                    }
                }

                lastSent.set(currentSent);
                lastTime.set(currentTime);
            }, 1, 1, TimeUnit.SECONDS);

            // 发送任务
            for (MqttClient client : clients) {
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        long endTime = startTime.get() + (TEST_DURATION_SECONDS * 1000L);

                        while (System.currentTimeMillis() < endTime) {
                            try {
                                client.publish(TOPIC, PAYLOAD, QOS, false, false);
                                sentCount.increment();
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

            endLatch.await(TEST_DURATION_SECONDS + 10, TimeUnit.SECONDS);
            statsExecutor.shutdown();

            // 等待最后的消息发送完成
            Thread.sleep(1000);

            long totalTime = System.currentTimeMillis() - startTime.get();
            long totalSent = sentCount.sum();
            double sendQps = totalSent * 1000.0 / totalTime;

            // 输出结果
            System.out.println("\n========================================");
            System.out.println("  发送测试结果");
            System.out.println("========================================");
            System.out.printf("客户端数量: %d%n", clients.size());
            System.out.printf("总发送: %,d%n", totalSent);
            System.out.printf("测试时长: %.2f 秒%n", totalTime / 1000.0);
            System.out.printf("发送 QPS: %,.0f/s%n", sendQps);
            if (QOS != MqttQoS.AT_MOST_ONCE) {
                System.out.printf("已确认: %,d%n", deliveredCount.sum());
            }
            System.out.println("========================================");

            // 断开连接
            System.out.println("\n正在断开连接...");
            CountDownLatch disconnectLatch = new CountDownLatch(clients.size());
            for (MqttClient client : clients) {
                try {
                    client.disconnect().onComplete(ar -> disconnectLatch.countDown());
                } catch (Exception e) {
                    disconnectLatch.countDown();
                }
            }
            disconnectLatch.await(30, TimeUnit.SECONDS);

            executor.shutdown();

        } finally {
            CountDownLatch closeLatch = new CountDownLatch(1);
            vertx.close().onComplete(ar -> closeLatch.countDown());
            closeLatch.await(5, TimeUnit.SECONDS);
        }

        System.out.println("测试完成");
    }
}
