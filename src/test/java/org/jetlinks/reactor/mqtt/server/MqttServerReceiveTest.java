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

import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * MQTT 服务器接收测试 - 独立运行，监控每秒处理消息数
 * <p>
 * 使用方式：
 * 1. 运行 main 方法启动服务器
 * 2. 在另一台机器运行 MqttClientSendTest 发送消息
 * 3. 观察控制台输出的 QPS 统计
 *
 * @author PengyuDeng
 */
public class MqttServerReceiveTest {

    // ================= 配置参数 =================
    private static final String HOST = "0.0.0.0";    // 监听地址
    private static final int PORT = 11883;           // 监听端口
    private static final int MAX_MESSAGE_SIZE = 65536; // 最大消息大小
    private static final int IDLE_TIMEOUT_SECONDS = 300; // 空闲超时
    // =============================================

    private static final LongAdder receivedMessages = new LongAdder();
    private static final LongAdder connectedClients = new LongAdder();

    public static void main(String[] args) throws Exception {
        System.out.println("========================================");
        System.out.println("  MQTT 服务器接收测试");
        System.out.println("========================================");
        System.out.println("监听地址: " + HOST + ":" + PORT);
        System.out.println("最大消息大小: " + MAX_MESSAGE_SIZE + " 字节");
        System.out.println("空闲超时: " + IDLE_TIMEOUT_SECONDS + " 秒");
        System.out.println("----------------------------------------");
        System.out.println("等待客户端连接并发送消息...");
        System.out.println("按 Ctrl+C 停止服务器");
        System.out.println("----------------------------------------\n");

        // 启动服务器
        DisposableServer server = MqttServer.create()
                                            .host(HOST)
                                            .port(PORT)
                                            .maxMessageSize(MAX_MESSAGE_SIZE)
                                            .idleTimeout(Duration.ofSeconds(IDLE_TIMEOUT_SECONDS))
                                            .handle(connection -> {
                                                connectedClients.increment();
                                                System.out.println("客户端连接: " + connection.getClientId() + " 来自 " + connection.getClientAddress());

                                                return connection.listener(new MqttMessageListener() {
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
                                                    public Mono<Void> onUnsubscribe(MqttUnsubscription unsubscription) {
                                                        return Mono.empty();
                                                    }

                                                    @Override
                                                    public Mono<Void> onDisconnect(MqttConnection conn) {
                                                        connectedClients.decrement();
                                                        System.out.println("客户端断开: " + conn.getClientId());
                                                        return Mono.empty();
                                                    }
                                                }).accept();
                                            })
                                            .bindNow();

        System.out.println("服务器已启动: tcp://" + HOST + ":" + PORT + "\n");

        // 启动统计线程
        ScheduledExecutorService statsExecutor = Executors.newSingleThreadScheduledExecutor();
        AtomicLong lastReceived = new AtomicLong(0);
        AtomicLong lastTime = new AtomicLong(System.currentTimeMillis());
        AtomicLong peakQps = new AtomicLong(0);
        AtomicLong totalReceived = new AtomicLong(0);
        AtomicLong statsStartTime = new AtomicLong(0);

        statsExecutor.scheduleAtFixedRate(() -> {
            long currentReceived = receivedMessages.sum();
            long currentTime = System.currentTimeMillis();
            long timeDiff = currentTime - lastTime.get();

            if (timeDiff > 0) {
                long qps = (currentReceived - lastReceived.get()) * 1000 / timeDiff;
                long clients = connectedClients.sum();

                if (qps > 0) {
                    // 记录峰值
                    if (qps > peakQps.get()) {
                        peakQps.set(qps);
                    }

                    // 记录开始时间
                    if (statsStartTime.get() == 0) {
                        statsStartTime.set(currentTime);
                    }

                    long runningSeconds = (currentTime - statsStartTime.get()) / 1000;
                    long avgQps = runningSeconds > 0 ? currentReceived / runningSeconds : qps;

                    System.out.printf("[%s] 接收: %,d/s | 峰值: %,d/s | 平均: %,d/s | 总接收: %,d | 客户端: %d%n",
                                      java.time.LocalTime.now().toString().substring(0, 8),
                                      qps, peakQps.get(), avgQps, currentReceived, clients);
                } else if (clients > 0) {
                    System.out.printf("[%s] 等待消息... | 客户端: %d%n",
                                      java.time.LocalTime.now().toString().substring(0, 8), clients);
                }
            }

            lastReceived.set(currentReceived);
            lastTime.set(currentTime);
        }, 1, 1, TimeUnit.SECONDS);

        // 注册关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n----------------------------------------");
            System.out.println("正在关闭服务器...");

            statsExecutor.shutdown();

            long totalTime = System.currentTimeMillis() - statsStartTime.get();
            long total = receivedMessages.sum();

            if (total > 0 && totalTime > 0) {
                System.out.println("\n========================================");
                System.out.println("  最终统计");
                System.out.println("========================================");
                System.out.printf("总接收消息: %,d%n", total);
                System.out.printf("运行时长: %.2f 秒%n", totalTime / 1000.0);
                System.out.printf("平均 QPS: %,.0f/s%n", total * 1000.0 / totalTime);
                System.out.printf("峰值 QPS: %,d/s%n", peakQps.get());
                System.out.println("========================================");
            }

            server.dispose();
            System.out.println("服务器已停止");
        }));

        // 阻塞主线程
        server.onDispose().block();
    }
}
