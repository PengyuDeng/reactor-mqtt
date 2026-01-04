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
import io.netty.handler.codec.mqtt.MqttQoS;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * MQTT 客户端发送测试 - 使用 MqttClient API
 * <p>
 * 使用方式：
 * 1. 修改 SERVER_HOST 为目标服务器 IP
 * 2. 运行 main 方法
 *
 * @author PengyuDeng
 */
public class MqttClientSendTest {

    // ================= 配置参数 =================
    private static final String SERVER_HOST = "127.0.0.1";  // 服务器地址
    private static final int SERVER_PORT = 11883;           // 服务器端口
    private static final int CLIENT_COUNT = 100;              // 客户端数量
    private static final int TEST_DURATION_SECONDS = 10;    // 测试时长（秒）
    private static final MqttQoS QOS = MqttQoS.AT_LEAST_ONCE; // QoS 级别
    private static final String TOPIC = "test/benchmark";   // 发送主题
    private static final byte[] PAYLOAD = "test-payload".getBytes(StandardCharsets.UTF_8);
    // =============================================

    private static final LongAdder sentCount = new LongAdder();
    private static final LongAdder connectedClients = new LongAdder();

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("  MQTT 客户端发送测试 (MqttClient API)");
        System.out.println("========================================");
        System.out.println("服务器: " + SERVER_HOST + ":" + SERVER_PORT);
        System.out.println("客户端数量: " + CLIENT_COUNT);
        System.out.println("QoS: " + QOS.value());
        System.out.println("测试时长: " + TEST_DURATION_SECONDS + " 秒");
        System.out.println("消息大小: " + PAYLOAD.length + " 字节");
        System.out.println("----------------------------------------\n");

        // 启动统计线程
        ScheduledExecutorService statsExecutor = Executors.newSingleThreadScheduledExecutor();
        AtomicLong lastSent = new AtomicLong(0);
        AtomicLong lastTime = new AtomicLong(System.currentTimeMillis());
        AtomicLong startTime = new AtomicLong(0);

        statsExecutor.scheduleAtFixedRate(() -> {
            long currentSent = sentCount.sum();
            long currentTime = System.currentTimeMillis();
            long timeDiff = currentTime - lastTime.get();

            if (timeDiff > 0 && startTime.get() > 0) {
                long sendQps = (currentSent - lastSent.get()) * 1000 / timeDiff;
                System.out.printf("  发送: %,d/s | 总发送: %,d | 客户端: %d%n",
                                  sendQps, currentSent, connectedClients.sum());
            }

            lastSent.set(currentSent);
            lastTime.set(currentTime);
        }, 1, 1, TimeUnit.SECONDS);

        System.out.println("正在连接客户端...\n");

        // 创建多个客户端并发送消息
        Flux.range(0, CLIENT_COUNT)
            .flatMap(i -> createClient("bench-client-" + i), CLIENT_COUNT)
            .doOnSubscribe(s -> startTime.set(System.currentTimeMillis()))
            .blockLast();

        statsExecutor.shutdown();

        // 输出结果
        long totalTime = System.currentTimeMillis() - startTime.get();
        long totalSent = sentCount.sum();
        double sendQps = totalSent * 1000.0 / totalTime;

        System.out.println("\n========================================");
        System.out.println("  发送测试结果");
        System.out.println("========================================");
        System.out.printf("客户端数量: %d%n", CLIENT_COUNT);
        System.out.printf("总发送: %,d%n", totalSent);
        System.out.printf("测试时长: %.2f 秒%n", totalTime / 1000.0);
        System.out.printf("发送 QPS: %,.0f/s%n", sendQps);
        System.out.println("========================================");
        System.out.println("测试完成");
    }

    /**
     * 创建一个 MQTT 客户端并发送消息
     */
    private static Mono<Void> createClient(String clientId) {
        return MqttClient.create()
                .host(SERVER_HOST)
                .port(SERVER_PORT)
                .clientId(clientId)
                .cleanSession(true)
                .keepAlive((short) 300)
                .qos(QOS)
                .connect()
                .doOnNext(conn -> {
                    System.out.println("客户端连接成功: " + clientId);
                    connectedClients.increment();
                })
                .flatMap(conn -> sendMessages(conn)
                        .doFinally(signal -> {
                            connectedClients.decrement();
                            conn.disconnect().subscribe();
                        }))
                .doOnError(e -> System.out.println("客户端 " + clientId + " 连接失败: " + e.getMessage()))
                .onErrorResume(e -> Mono.empty());
    }

    /**
     * 在指定时间内持续发送消息
     */
    private static Mono<Void> sendMessages(MqttClientConnection conn) {
        long endTime = System.currentTimeMillis() + (TEST_DURATION_SECONDS * 1000L);

        return Flux.generate(sink -> {
                       if (System.currentTimeMillis() < endTime) {
                           sink.next(1);
                       } else {
                           sink.complete();
                       }
                   })
                   .flatMap(i -> conn.publish(TOPIC, Unpooled.wrappedBuffer(PAYLOAD), QOS)
                                     .doOnSuccess(v -> sentCount.increment()), 256)
                   .then();
    }
}
