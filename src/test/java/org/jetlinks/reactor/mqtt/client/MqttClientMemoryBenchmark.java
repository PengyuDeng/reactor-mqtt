package org.jetlinks.reactor.mqtt.client;

import org.jetlinks.reactor.mqtt.server.*;
import org.openjdk.jol.info.ClassLayout;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 内存占用测试 - 纯响应式实现
 */
class MqttClientMemoryBenchmark {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 23883;
    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    private DisposableServer server;
    private final List<ServerConnection> serverConnections = new CopyOnWriteArrayList<>();

    @AfterEach
    void tearDown() {
        if (server != null && !server.isDisposed()) {
            server.disposeNow();
            System.out.println("MQTT 服务器已停止");
        }
    }

    @Test
    void analyzeClientConnectionLayout() {
        System.out.println("=== DefaultMqttClientConnection 对象布局 ===");
        System.out.println(ClassLayout.parseClass(DefaultClientConnection.class).toPrintable());
    }

    @Test
    void analyzeServerConnectionLayout() {
        System.out.println("=== DefaultMqttConnection 对象布局 ===");
        System.out.println(ClassLayout.parseClass(DefaultServerConnection.class).toPrintable());
    }

    /**
     * 启动服务器
     */
    private Mono<DisposableServer> startServer() {
        serverConnections.clear();

        return MqttServer.create()
                .host(HOST)
                .port(PORT)
                .handle(conn -> {
                    serverConnections.add(conn);
                    return conn.accept();
                })
                .bind()
                .doOnSuccess(s -> {
                    server = s;
                    System.out.println("MQTT 服务器已启动: tcp://" + HOST + ":" + PORT);
                })
                .cast(DisposableServer.class);
    }

    /**
     * 使用 MqttClient 创建客户端连接
     */
    private Mono<ClientConnection> createClient(String clientId) {
        return MqttClient.create()
                .host(HOST)
                .port(PORT)
                .clientId(clientId)
                .connect();
    }

    /**
     * 测试内存占用 - 响应式方式
     */
    @Test
    void measureClientConnectionMemory() {
        int clientCount = 500;
        List<ClientConnection> clientConnections = new CopyOnWriteArrayList<>();
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);
        AtomicLong memoryBefore = new AtomicLong();
        AtomicLong memoryAfter = new AtomicLong();

        StepVerifier.create(
                startServer()
                        .doOnSuccess(s -> {
                            forceGc();
                            memoryBefore.set(getUsedMemory());
                            System.out.println("=== 客户端连接内存测试 ===");
                            System.out.println("目标客户端数量: " + clientCount);
                        })
                        .thenMany(Flux.range(0, clientCount)
                                .flatMap(i -> createClient("client-" + i)
                                                .doOnSuccess(conn -> {
                                                    clientConnections.add(conn);
                                                    successCount.incrementAndGet();
                                                })
                                                .doOnError(e -> failCount.incrementAndGet())
                                                .onErrorResume(e -> Mono.empty()),
                                        50)  // 并发度
                        )
                        .then(Mono.delay(Duration.ofMillis(500)))  // 等待稳定
                        .doOnSuccess(v -> {
                            forceGc();
                            memoryAfter.set(getUsedMemory());
                        })
                        .then(Mono.fromRunnable(() -> {
                            // 输出统计
                            long totalMemory = memoryAfter.get() - memoryBefore.get();
                            int totalConnections = successCount.get();
                            double avgMemoryPerPair = totalConnections > 0 ? (double) totalMemory / totalConnections : 0;

                            System.out.println();
                            System.out.println("=== 内存统计 ===");
                            System.out.println("成功连接: " + successCount.get());
                            System.out.println("失败连接: " + failCount.get());
                            System.out.println("服务端连接数: " + serverConnections.size());
                            System.out.println("内存占用前: " + formatBytes(memoryBefore.get()));
                            System.out.println("内存占用后: " + formatBytes(memoryAfter.get()));
                            System.out.println("总增量: " + formatBytes(totalMemory));
                            System.out.println("平均每对连接(客户端+服务端): " + formatBytes((long) avgMemoryPerPair));
                            System.out.println("平均每端估算: " + formatBytes((long) (avgMemoryPerPair / 2)));

                            // 对象布局分析
                            if (!clientConnections.isEmpty()) {
                                System.out.println();
                                System.out.println("=== DefaultMqttClientConnection 实例布局 ===");
                                System.out.println(ClassLayout.parseInstance(clientConnections.get(0)).toPrintable());
                            }

                            if (!serverConnections.isEmpty()) {
                                System.out.println();
                                System.out.println("=== DefaultMqttConnection 实例布局 ===");
                                System.out.println(ClassLayout.parseInstance(serverConnections.get(0)).toPrintable());
                            }
                        }))
                        // 清理连接
                        .thenMany(Flux.fromIterable(clientConnections)
                                .flatMap(conn -> conn.close().onErrorResume(e -> Mono.empty()), 50))
                        .then()
        ).expectComplete().verify(TIMEOUT);

        System.out.println();
        System.out.println("测试完成");
    }

    /**
     * 单个连接详细分析
     */
    @Test
    void measureSingleConnectionMemory() {
        StepVerifier.create(
                startServer()
                        .then(createClient("test-client"))
                        .doOnSuccess(client -> {
                            System.out.println("=== DefaultMqttClientConnection 详细布局 ===");
                            System.out.println(ClassLayout.parseInstance(client).toPrintable());
                            System.out.println("实例大小: 56 bytes (不含引用对象)");
                        })
                        .flatMap(ClientConnection::close)
        ).expectComplete().verify(TIMEOUT);
    }

    private void forceGc() {
        System.gc();
        System.gc();
        try {
            Thread.sleep(100);
        } catch (InterruptedException ignored) {
        }
        System.gc();
    }

    private long getUsedMemory() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private String formatBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.2f KB", bytes / 1024.0);
        } else {
            return String.format("%.2f MB", bytes / (1024.0 * 1024.0));
        }
    }
}
