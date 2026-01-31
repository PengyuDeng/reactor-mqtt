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
package org.jetlinks.reactor.mqtt.performance;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.jetlinks.reactor.mqtt.client.ClientConnection;
import org.jetlinks.reactor.mqtt.client.MqttClient;
import org.jetlinks.reactor.mqtt.server.MqttServer;
import org.jetlinks.reactor.mqtt.server.ServerConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.netty.DisposableServer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 客户端和服务端压力测试
 *
 * <p>分别测试客户端->服务端 和 服务端->客户端 的吞吐量和性能</p>
 *
 * @author PengyuDeng
 */
public class SimpleClientServerStressTest {

    private static final int TEST_PORT = 11886;
    private static final String TEST_HOST = "127.0.0.1";
    private static final int TEST_DURATION_SECONDS = 30;
    private static final int WARMUP_SECONDS = 3;
    private static final int STATS_INTERVAL_SECONDS = 1; // 每秒显示一次统计
    private static final String CLIENT_TO_SERVER_TOPIC = "/stress/c2s";
    private static final String SERVER_TO_CLIENT_TOPIC = "/stress/s2c";
    private static final int CONCURRENT_SENDERS = Runtime.getRuntime().availableProcessors(); // CPU 核心数
    private static final int CONCURRENCY_PER_SENDER = 256; // 每个发送器的并发度

    private DisposableServer server;
    private final AtomicLong serverReceivedCount = new AtomicLong(0);
    private ServerConnection serverConnection; // 保存服务端连接用于服务端发送测试

    @BeforeEach
    void setup() {
        System.out.println("正在启动 MQTT 服务器...");

        // 创建并启动服务器
        server = MqttServer.create()
                .port(TEST_PORT)
                .handle(connection -> {
                    // 保存服务端连接
                    serverConnection = connection;

                    // 处理客户端发布的消息
                    connection.handlePublishing(msg -> {
                        serverReceivedCount.incrementAndGet();
                        return Mono.empty();
                    });

                    // 接受连接
                    return connection.accept();
                })
                .bind()
                .block(Duration.ofSeconds(5));

        System.out.println("MQTT 服务器已启动，监听端口: " + TEST_PORT);
    }

    @AfterEach
    void cleanup() {
        if (server != null && !server.isDisposed()) {
            System.out.println("正在关闭服务器...");
            server.disposeNow(Duration.ofSeconds(2));
            System.out.println("服务器已关闭");
        }
    }

    /**
     * 测试1：客户端发送到服务端（Client -> Server）
     */
    @Test
    void testClientToServerQoS1() {
        System.out.println("\n========================================");
        System.out.println("压力测试：客户端 -> 服务端 (QoS 1)");
        System.out.println("========================================");
        System.out.println("性能配置:");
        System.out.printf("  - CPU 核心数: %d%n", Runtime.getRuntime().availableProcessors());
        System.out.printf("  - 并行发送器数量: %d%n", CONCURRENT_SENDERS);
        System.out.printf("  - 每个发送器并发度: %d%n", CONCURRENCY_PER_SENDER);
        System.out.printf("  - 总并发度: %d%n", CONCURRENT_SENDERS * CONCURRENCY_PER_SENDER);
        System.out.println();

        runClientToServerTest().block();
    }

    /**
     * 测试2：服务端发送到客户端（Server -> Client）
     */
    @Test
    void testServerToClientQoS1() {
        System.out.println("\n========================================");
        System.out.println("压力测试：服务端 -> 客户端 (QoS 1)");
        System.out.println("========================================");
        System.out.println("性能配置:");
        System.out.printf("  - CPU 核心数: %d%n", Runtime.getRuntime().availableProcessors());
        System.out.printf("  - 并行发送器数量: %d%n", CONCURRENT_SENDERS);
        System.out.printf("  - 每个发送器并发度: %d%n", CONCURRENCY_PER_SENDER);
        System.out.printf("  - 总并发度: %d%n", CONCURRENT_SENDERS * CONCURRENCY_PER_SENDER);
        System.out.println();

        runServerToClientTest().block();
    }

    /**
     * 执行客户端到服务端的压力测试
     */
    private Mono<Void> runClientToServerTest() {
        AtomicLong clientSentCount = new AtomicLong(0);
        AtomicBoolean testRunning = new AtomicBoolean(false);
        AtomicBoolean statsRunning = new AtomicBoolean(false);

        System.out.println("正在连接到服务器...");

        // 1. 创建客户端连接
        return MqttClient.create()
                .host(TEST_HOST)
                .port(TEST_PORT)
                .clientId("stress-client-c2s")
                .publishTimeout(Duration.ofSeconds(5))
                .connect()
                .flatMap(client -> {
                    System.out.println("客户端已连接");
                    System.out.println("预热 " + WARMUP_SECONDS + " 秒...");

                    // 2. 启动消息发送流
                    testRunning.set(true);

                    // 3. 预热阶段
                    return Mono.delay(Duration.ofSeconds(WARMUP_SECONDS))
                            .doOnNext(v -> {
                                // 重置计数器
                                clientSentCount.set(0);
                                serverReceivedCount.set(0);
                                System.out.println("预热完成，开始正式测试 " + TEST_DURATION_SECONDS + " 秒...");
                                System.out.println("时间(秒) | 客户端发送QPS | 服务端接收QPS | 丢失率");
                                System.out.println("--------|--------------|--------------|-------");
                                // 启动实时统计
                                statsRunning.set(true);
                            })
                            .then(Mono.defer(() -> {
                                // 4. 并行启动多个发送器和统计器
                                Flux<Void> senderFlux = Flux.range(0, CONCURRENT_SENDERS)
                                        .flatMap(i -> startClientSender(client, CLIENT_TO_SERVER_TOPIC,
                                                clientSentCount, testRunning)
                                                .subscribeOn(Schedulers.parallel()));

                                Mono<Void> statsFlux = startClientToServerStats(
                                        clientSentCount,
                                        serverReceivedCount,
                                        statsRunning
                                );

                                // 5. 等待测试时长
                                Mono<Void> testTimer = Mono.delay(Duration.ofSeconds(TEST_DURATION_SECONDS))
                                        .doOnNext(v -> {
                                            testRunning.set(false);
                                            statsRunning.set(false);
                                            System.out.println("\n测试完成，等待剩余消息处理...");
                                        })
                                        .then();

                                // 6. 合并所有流
                                return Mono.when(
                                        senderFlux.onErrorResume(e -> Mono.empty()).then(),
                                        statsFlux.onErrorResume(e -> Mono.empty()),
                                        testTimer
                                );
                            }))
                            .then(Mono.delay(Duration.ofSeconds(2)))
                            .then(Mono.fromCallable(() -> new ClientToServerTestResult(
                                    TEST_DURATION_SECONDS,
                                    clientSentCount.get(),
                                    serverReceivedCount.get()
                            )))
                            .doOnNext(this::printClientToServerResults)
                            .then(client.disconnect()
                                    .timeout(Duration.ofSeconds(2))
                                    .onErrorResume(e -> Mono.empty()));
                });
    }

    /**
     * 执行服务端到客户端的压力测试
     */
    private Mono<Void> runServerToClientTest() {
        AtomicLong serverSentCount = new AtomicLong(0);
        AtomicLong clientReceivedCount = new AtomicLong(0);
        AtomicBoolean testRunning = new AtomicBoolean(false);
        AtomicBoolean statsRunning = new AtomicBoolean(false);

        System.out.println("正在连接到服务器...");

        // 1. 创建客户端连接并订阅
        return MqttClient.create()
                .host(TEST_HOST)
                .port(TEST_PORT)
                .clientId("stress-client-s2c")
                .publishTimeout(Duration.ofSeconds(5))
                .connect()
                .flatMap(client -> {
                    System.out.println("客户端已连接，订阅主题: " + SERVER_TO_CLIENT_TOPIC);

                    // 2. 订阅主题
                    client.subscribe(SERVER_TO_CLIENT_TOPIC, MqttQoS.AT_LEAST_ONCE, msg -> {
                        clientReceivedCount.incrementAndGet();
                        return msg.ack();
                    });

                    System.out.println("预热 " + WARMUP_SECONDS + " 秒...");

                    // 3. 启动消息发送流
                    testRunning.set(true);

                    // 4. 预热阶段
                    return Mono.delay(Duration.ofSeconds(WARMUP_SECONDS))
                            .doOnNext(v -> {
                                // 重置计数器
                                serverSentCount.set(0);
                                clientReceivedCount.set(0);
                                System.out.println("预热完成，开始正式测试 " + TEST_DURATION_SECONDS + " 秒...");
                                System.out.println("时间(秒) | 服务端发送QPS | 客户端接收QPS | 丢失率");
                                System.out.println("--------|--------------|--------------|-------");
                                // 启动实时统计
                                statsRunning.set(true);
                            })
                            .then(Mono.defer(() -> {
                                // 5. 并行启动多个发送器和统计器
                                Flux<Void> senderFlux = Flux.range(0, CONCURRENT_SENDERS)
                                        .flatMap(i -> startServerSender(serverConnection, SERVER_TO_CLIENT_TOPIC,
                                                serverSentCount, testRunning)
                                                .subscribeOn(Schedulers.parallel()));

                                Mono<Void> statsFlux = startServerToClientStats(
                                        serverSentCount,
                                        clientReceivedCount,
                                        statsRunning
                                );

                                // 6. 等待测试时长
                                Mono<Void> testTimer = Mono.delay(Duration.ofSeconds(TEST_DURATION_SECONDS))
                                        .doOnNext(v -> {
                                            testRunning.set(false);
                                            statsRunning.set(false);
                                            System.out.println("\n测试完成，等待剩余消息处理...");
                                        })
                                        .then();

                                // 7. 合并所有流
                                return Mono.when(
                                        senderFlux.onErrorResume(e -> Mono.empty()).then(),
                                        statsFlux.onErrorResume(e -> Mono.empty()),
                                        testTimer
                                );
                            }))
                            .then(Mono.delay(Duration.ofSeconds(2)))
                            .then(Mono.fromCallable(() -> new ServerToClientTestResult(
                                    TEST_DURATION_SECONDS,
                                    serverSentCount.get(),
                                    clientReceivedCount.get()
                            )))
                            .doOnNext(this::printServerToClientResults)
                            .then(client.disconnect()
                                    .timeout(Duration.ofSeconds(2))
                                    .onErrorResume(e -> Mono.empty()));
                });
    }

    /**
     * 客户端发送器
     */
    private Flux<Void> startClientSender(ClientConnection client, String topic,
                                         AtomicLong sentCount, AtomicBoolean running) {
        return Flux.generate(() -> 0L, (state, sink) -> {
                    if (running.get()) {
                        sink.next(state);
                        return state + 1;
                    } else {
                        sink.complete();
                        return state;
                    }
                })
                .cast(Long.class)
                .flatMap(msgId -> {
                    byte[] payload = ("message-" + msgId).getBytes(StandardCharsets.UTF_8);
                    return client.publish(topic,
                                        Unpooled.wrappedBuffer(payload),
                                        MqttQoS.AT_LEAST_ONCE,
                                        false)
                            .doOnSuccess(v -> sentCount.incrementAndGet())
                            .onErrorResume(e -> Mono.empty());
                }, CONCURRENCY_PER_SENDER);
    }

    /**
     * 服务端发送器
     */
    private Flux<Void> startServerSender(ServerConnection server, String topic,
                                         AtomicLong sentCount, AtomicBoolean running) {
        // 检查是否是DefaultServerConnection类型
        if (!(server instanceof org.jetlinks.reactor.mqtt.server.DefaultServerConnection)) {
            return Flux.error(new IllegalArgumentException("Server must be DefaultServerConnection"));
        }

        org.jetlinks.reactor.mqtt.server.DefaultServerConnection defaultServer =
                (org.jetlinks.reactor.mqtt.server.DefaultServerConnection) server;

        return Flux.generate(() -> 0L, (state, sink) -> {
                    if (running.get()) {
                        sink.next(state);
                        return state + 1;
                    } else {
                        sink.complete();
                        return state;
                    }
                })
                .cast(Long.class)
                .flatMap(msgId -> {
                    byte[] payload = ("message-" + msgId).getBytes(StandardCharsets.UTF_8);
                    return defaultServer.publish(topic,
                                        Unpooled.wrappedBuffer(payload),
                                        MqttQoS.AT_LEAST_ONCE,
                                        false)
                            .doOnSuccess(v -> sentCount.incrementAndGet())
                            .onErrorResume(e -> Mono.empty());
                }, CONCURRENCY_PER_SENDER);
    }

    /**
     * 客户端到服务端实时统计
     */
    private Mono<Void> startClientToServerStats(AtomicLong clientSentCount,
                                                 AtomicLong serverReceivedCount,
                                                 AtomicBoolean running) {
        AtomicLong lastClientSent = new AtomicLong(0);
        AtomicLong lastServerReceived = new AtomicLong(0);
        AtomicLong elapsedSeconds = new AtomicLong(0);

        return Flux.interval(Duration.ofSeconds(STATS_INTERVAL_SECONDS))
                .takeWhile(tick -> running.get())
                .doOnNext(tick -> {
                    if (!running.get()) {
                        return;
                    }

                    elapsedSeconds.incrementAndGet();

                    long currentClientSent = clientSentCount.get();
                    long currentServerReceived = serverReceivedCount.get();

                    long clientSendQps = currentClientSent - lastClientSent.get();
                    long serverReceiveQps = currentServerReceived - lastServerReceived.get();

                    double lossRate = currentClientSent == 0 ? 0 :
                            (1 - (double) currentServerReceived / currentClientSent) * 100;

                    System.out.printf("%7d | %,12d | %,12d | %5.2f%%%n",
                            elapsedSeconds.get(),
                            clientSendQps,
                            serverReceiveQps,
                            lossRate
                    );

                    lastClientSent.set(currentClientSent);
                    lastServerReceived.set(currentServerReceived);
                })
                .then();
    }

    /**
     * 服务端到客户端实时统计
     */
    private Mono<Void> startServerToClientStats(AtomicLong serverSentCount,
                                                 AtomicLong clientReceivedCount,
                                                 AtomicBoolean running) {
        AtomicLong lastServerSent = new AtomicLong(0);
        AtomicLong lastClientReceived = new AtomicLong(0);
        AtomicLong elapsedSeconds = new AtomicLong(0);

        return Flux.interval(Duration.ofSeconds(STATS_INTERVAL_SECONDS))
                .takeWhile(tick -> running.get())
                .doOnNext(tick -> {
                    if (!running.get()) {
                        return;
                    }

                    elapsedSeconds.incrementAndGet();

                    long currentServerSent = serverSentCount.get();
                    long currentClientReceived = clientReceivedCount.get();

                    long serverSendQps = currentServerSent - lastServerSent.get();
                    long clientReceiveQps = currentClientReceived - lastClientReceived.get();

                    double lossRate = currentServerSent == 0 ? 0 :
                            (1 - (double) currentClientReceived / currentServerSent) * 100;

                    System.out.printf("%7d | %,12d | %,12d | %5.2f%%%n",
                            elapsedSeconds.get(),
                            serverSendQps,
                            clientReceiveQps,
                            lossRate
                    );

                    lastServerSent.set(currentServerSent);
                    lastClientReceived.set(currentClientReceived);
                })
                .then();
    }

    /**
     * 客户端到服务端测试结果
     */
    private record ClientToServerTestResult(
            double durationSeconds,
            long clientSentCount,
            long serverReceivedCount
    ) {
        double clientSendQps() {
            return clientSentCount / durationSeconds;
        }

        double serverReceiveQps() {
            return serverReceivedCount / durationSeconds;
        }

        double lossRate() {
            return clientSentCount == 0 ? 0 :
                    (1 - (double) serverReceivedCount / clientSentCount) * 100;
        }
    }

    /**
     * 服务端到客户端测试结果
     */
    private record ServerToClientTestResult(
            double durationSeconds,
            long serverSentCount,
            long clientReceivedCount
    ) {
        double serverSendQps() {
            return serverSentCount / durationSeconds;
        }

        double clientReceiveQps() {
            return clientReceivedCount / durationSeconds;
        }

        double lossRate() {
            return serverSentCount == 0 ? 0 :
                    (1 - (double) clientReceivedCount / serverSentCount) * 100;
        }
    }

    /**
     * 打印客户端到服务端测试结果
     */
    private void printClientToServerResults(ClientToServerTestResult result) {
        System.out.println("\n========================================");
        System.out.println("测试结果：客户端 -> 服务端");
        System.out.println("========================================");
        System.out.printf("测试时长:           %.1f 秒%n", result.durationSeconds);
        System.out.printf("客户端发送总数:     %,d%n", result.clientSentCount);
        System.out.printf("服务端接收总数:     %,d%n", result.serverReceivedCount);
        System.out.printf("客户端发送 QPS:     %,.2f 消息/秒%n", result.clientSendQps());
        System.out.printf("服务端接收 QPS:     %,.2f 消息/秒%n", result.serverReceiveQps());
        System.out.printf("消息丢失率:         %.2f%%%n", result.lossRate());
        System.out.println("========================================\n");
    }

    /**
     * 打印服务端到客户端测试结果
     */
    private void printServerToClientResults(ServerToClientTestResult result) {
        System.out.println("\n========================================");
        System.out.println("测试结果：服务端 -> 客户端");
        System.out.println("========================================");
        System.out.printf("测试时长:           %.1f 秒%n", result.durationSeconds);
        System.out.printf("服务端发送总数:     %,d%n", result.serverSentCount);
        System.out.printf("客户端接收总数:     %,d%n", result.clientReceivedCount);
        System.out.printf("服务端发送 QPS:     %,.2f 消息/秒%n", result.serverSendQps());
        System.out.printf("客户端接收 QPS:     %,.2f 消息/秒%n", result.clientReceiveQps());
        System.out.printf("消息丢失率:         %.2f%%%n", result.lossRate());
        System.out.println("========================================\n");
    }
}
