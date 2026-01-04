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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.*;
import org.junit.jupiter.api.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.Connection;
import reactor.netty.DisposableServer;
import reactor.netty.tcp.TcpClient;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MQTT 服务器压力测试 - 纯响应式实现
 */
class MqttServerStressTest {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 36544;
    private static final int MAX_MESSAGE_SIZE = 65536;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private DisposableServer server;
    private final AtomicInteger receivedMessages = new AtomicInteger(0);
    private final AtomicInteger connectedClients = new AtomicInteger(0);

    @AfterEach
    void tearDown() {
        if (server != null && !server.isDisposed()) {
            server.disposeNow();
        }
    }

    /**
     * 创建并启动 MQTT 服务器
     */
    private Mono<DisposableServer> startServer() {
        receivedMessages.set(0);
        connectedClients.set(0);

        return MqttServer.create()
                         .host(HOST)
                         .port(PORT)
                         .maxMessageSize(MAX_MESSAGE_SIZE)
                         .idleTimeout(Duration.ofSeconds(60))
                         .handle(connection -> {
                             connectedClients.incrementAndGet();

                             connection.onClose()
                                       .doOnSuccess(v -> connectedClients.decrementAndGet())
                                       .subscribe();

                             return connection.listener(new MqttMessageListener() {
                                 @Override
                                 public Mono<Void> onPublish(ServerReceivedPublish message) {
                                     receivedMessages.incrementAndGet();
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
                                 public Mono<Void> onDisconnect(ServerConnection conn) {
                                     return Mono.empty();
                                 }
                             }).accept();
                         })
                         .bind()
                         .map(s -> {
                             server = s;
                             System.out.println("MQTT 服务器已启动: tcp://" + HOST + ":" + PORT);
                             return s;
                         });
    }

    /**
     * 响应式创建客户端连接
     */
    private Mono<Connection> createClient(String clientId) {
        Sinks.One<Connection> connectionSink = Sinks.one();

        return TcpClient.create()
                        .host(HOST)
                        .port(PORT)
                        .doOnConnected(c -> {
                            c.addHandlerFirst("mqtt-encoder", MqttEncoder.INSTANCE);
                            c.addHandlerFirst("mqtt-decoder", new MqttDecoder(MAX_MESSAGE_SIZE));
                        })
                        .handle((inbound, outbound) -> {
                            MqttConnectMessage connectMessage = MqttMessageBuilders.connect()
                                                                                   .clientId(clientId)
                                                                                   .cleanSession(true)
                                                                                   .keepAlive(300)
                                                                                   .build();

                            // 监听 CONNACK
                            inbound.receiveObject()
                                   .cast(MqttMessage.class)
                                   .filter(msg -> msg.fixedHeader().messageType() == MqttMessageType.CONNACK)
                                   .next()
                                   .subscribe(msg -> {
                                       MqttConnAckMessage connAck = (MqttConnAckMessage) msg;
                                       if (connAck
                                               .variableHeader()
                                               .connectReturnCode() == MqttConnectReturnCode.CONNECTION_ACCEPTED) {
                                           if (inbound instanceof Connection) {
                                               connectionSink.tryEmitValue((Connection) inbound);
                                           }
                                       } else {
                                           connectionSink.tryEmitError(new RuntimeException("连接被拒绝"));
                                       }
                                   });

                            // 发送 CONNECT
                            return outbound.sendObject(Mono.just(connectMessage))
                                           .then(Mono.never());
                        })
                        .connect()
                        .then(connectionSink.asMono().timeout(Duration.ofSeconds(10)));
    }

    /**
     * 响应式发布消息
     */
    private Mono<Void> publish(Connection conn, String topic, byte[] payload, MqttQoS qos, AtomicInteger messageIdGen) {
        int messageId = qos == MqttQoS.AT_MOST_ONCE ? 0 : messageIdGen.getAndIncrement() & 0xFFFF;
        if (messageId == 0 && qos != MqttQoS.AT_MOST_ONCE) {
            messageId = messageIdGen.getAndIncrement() & 0xFFFF;
        }

        ByteBuf payloadBuf = Unpooled.wrappedBuffer(payload);
        MqttPublishMessage publishMessage = new MqttPublishMessage(
                new MqttFixedHeader(MqttMessageType.PUBLISH, false, qos, false, 0),
                new MqttPublishVariableHeader(topic, messageId),
                payloadBuf
        );

        return conn.outbound().sendObject(Mono.just(publishMessage)).then();
    }

    /**
     * 响应式断开连接
     */
    private Mono<Void> disconnect(Connection conn) {
        MqttMessage disconnectMessage = new MqttMessage(
                new MqttFixedHeader(MqttMessageType.DISCONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0)
        );
        return conn.outbound().sendObject(Mono.just(disconnectMessage)).then()
                   .doFinally(signal -> conn.dispose());
    }

    /**
     * 测试并发连接
     */
    @Test
    void testConcurrentConnections() {
        int clientCount = 100;
        List<Connection> clients = new CopyOnWriteArrayList<>();
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);
        AtomicLong startTime = new AtomicLong();

        StepVerifier.create(
            startServer()
                    .doOnSuccess(s -> startTime.set(System.currentTimeMillis()))
                    .thenMany(
                        // 并发创建客户端
                        Flux.range(0, clientCount)
                            .flatMap(i -> createClient("stress-conn-" + i)
                                    .doOnSuccess(conn -> {
                                        clients.add(conn);
                                        successCount.incrementAndGet();
                                    })
                                    .onErrorResume(e -> {
                                        failCount.incrementAndGet();
                                        return Mono.empty();
                                    }), 20) // 并发度 20
                    )
                    .then(Mono.fromRunnable(() -> {
                        long elapsed = System.currentTimeMillis() - startTime.get();

                        System.out.println("=== 并发连接测试结果 ===");
                        System.out.println("总客户端数: " + clientCount);
                        System.out.println("成功连接: " + successCount.get());
                        System.out.println("连接失败: " + failCount.get());
                        System.out.println("耗时: " + elapsed + " ms");
                        System.out.println("连接速率: " + (clientCount * 1000.0 / elapsed) + " 连接/秒");

                        assertEquals(clientCount, successCount.get());
                        assertEquals(clientCount, connectedClients.get());
                    }))
                    .thenMany(
                        // 断开所有客户端
                        Flux.fromIterable(clients)
                            .flatMap(this::disconnect)
                    )
                    .then(Mono.fromRunnable(() -> System.out.println("MQTT 服务器已停止")))
        )
        .expectComplete()
        .verify(Duration.ofSeconds(60));
    }

    /**
     * 测试高吞吐量消息发布
     */
    @Test
    void testHighThroughputPublish() {
        int messageCount = 10000;
        String topic = "stress/throughput";
        byte[] payload = "Hello MQTT Stress Test".getBytes(StandardCharsets.UTF_8);
        AtomicInteger messageIdGen = new AtomicInteger(1);
        AtomicLong startTime = new AtomicLong();

        StepVerifier.create(
            startServer()
                    .doOnSuccess(s -> startTime.set(System.currentTimeMillis()))
                    .flatMap(s -> createClient("stress-throughput")
                            .flatMap(conn -> Flux.range(0, messageCount)
                                                 .flatMap(i -> publish(conn, topic, payload, MqttQoS.AT_MOST_ONCE, messageIdGen), 256)
                                                 .then()
                                                 .delayElement(Duration.ofSeconds(2)) // 等待服务器处理
                                                 .then(disconnect(conn))
                                                 .thenReturn(conn)))
                    .flatMap(conn -> Mono.fromRunnable(() -> {
                        long elapsed = System.currentTimeMillis() - startTime.get();

                        System.out.println("=== 高吞吐量测试结果 ===");
                        System.out.println("发送消息数: " + messageCount);
                        System.out.println("接收消息数: " + receivedMessages.get());
                        System.out.println("耗时: " + elapsed + " ms");
                        System.out.println("吞吐量: " + (messageCount * 1000.0 / elapsed) + " 消息/秒");

                        assertTrue(receivedMessages.get() >= messageCount * 0.95, "至少应接收95%的消息");
                        System.out.println("MQTT 服务器已停止");
                    }))
        )
        .expectComplete()
        .verify(TIMEOUT);
    }

    /**
     * 测试多客户端并发发布
     */
    @Test
    void testConcurrentPublish() {
        int clientCount = 10;
        int messagesPerClient = 1000;
        int totalMessages = clientCount * messagesPerClient;
        String topic = "stress/concurrent";
        byte[] payload = "Concurrent message".getBytes(StandardCharsets.UTF_8);

        List<Connection> clients = new CopyOnWriteArrayList<>();
        AtomicLong startTime = new AtomicLong();

        StepVerifier.create(
            startServer()
                    .thenMany(
                        // 连接客户端
                        Flux.range(0, clientCount)
                            .flatMap(i -> createClient("stress-pub-" + i)
                                    .doOnSuccess(clients::add)
                                    .onErrorResume(e -> Mono.empty()), 20))
                    .then(Mono.fromRunnable(() -> {
                        assertEquals(clientCount, clients.size());
                        startTime.set(System.currentTimeMillis());
                    }))
                    .thenMany(
                        // 并发发布
                        Flux.fromIterable(clients)
                            .flatMap(conn -> {
                                AtomicInteger messageIdGen = new AtomicInteger(1);
                                return Flux.range(0, messagesPerClient)
                                           .flatMap(j -> publish(conn, topic, payload, MqttQoS.AT_MOST_ONCE, messageIdGen), 64)
                                           .then();
                            }, clientCount)
                    )
                    .then(Mono.delay(Duration.ofSeconds(2))) // 等待服务器处理
                    .then(Mono.fromRunnable(() -> {
                        long elapsed = System.currentTimeMillis() - startTime.get();

                        System.out.println("=== 并发发布测试结果 ===");
                        System.out.println("客户端数: " + clientCount);
                        System.out.println("每客户端消息数: " + messagesPerClient);
                        System.out.println("总消息数: " + totalMessages);
                        System.out.println("接收消息数: " + receivedMessages.get());
                        System.out.println("耗时: " + elapsed + " ms");
                        System.out.println("吞吐量: " + (totalMessages * 1000.0 / elapsed) + " 消息/秒");

                        assertTrue(receivedMessages.get() >= totalMessages * 0.95);
                    }))
                    .thenMany(
                        // 断开连接
                        Flux.fromIterable(clients)
                            .flatMap(this::disconnect))
                    .then(Mono.fromRunnable(() -> System.out.println("MQTT 服务器已停止")))
        )
        .expectComplete()
        .verify(Duration.ofSeconds(60));
    }

    /**
     * 测试 QoS 1 消息确认
     */
    @Test
    void testQoS1Acknowledgment() {
        int messageCount = 1000;
        String topic = "stress/qos1";
        byte[] payload = "QoS 1 message".getBytes(StandardCharsets.UTF_8);
        AtomicInteger messageIdGen = new AtomicInteger(1);
        AtomicLong startTime = new AtomicLong();

        StepVerifier.create(
            startServer()
                    .doOnSuccess(s -> startTime.set(System.currentTimeMillis()))
                    .flatMap(s -> createClient("stress-qos1")
                            .flatMap(conn ->
                                // 同步发送所有消息
                                Flux.range(0, messageCount)
                                    .flatMap(i -> publish(conn, topic, payload, MqttQoS.AT_LEAST_ONCE, messageIdGen), 64)
                                    .then()
                                    // 等待服务器处理
                                    .then(Mono.delay(Duration.ofSeconds(2)))
                                    .then(disconnect(conn))
                                    .thenReturn(conn)))
                    .flatMap(conn -> Mono.fromRunnable(() -> {
                        long elapsed = System.currentTimeMillis() - startTime.get();

                        System.out.println("=== QoS 1 测试结果 ===");
                        System.out.println("发送消息数: " + messageCount);
                        System.out.println("服务器接收数: " + receivedMessages.get());
                        System.out.println("耗时: " + elapsed + " ms");

                        assertEquals(messageCount, receivedMessages.get(), "所有消息应被接收");
                        System.out.println("MQTT 服务器已停止");
                    }))
        )
        .expectComplete()
        .verify(TIMEOUT);
    }

    /**
     * 测试持续压力
     */
    @Test
    void testSustainedLoad() {
        int durationSeconds = 10;
        int clientCount = 5;
        String topic = "stress/sustained";
        byte[] payload = "Sustained load message".getBytes(StandardCharsets.UTF_8);
        AtomicLong sentCount = new AtomicLong(0);

        List<Connection> clients = new CopyOnWriteArrayList<>();
        AtomicLong endTime = new AtomicLong();

        StepVerifier.create(
            startServer()
                    .thenMany(
                        // 连接客户端
                        Flux.range(0, clientCount)
                            .flatMap(i -> createClient("stress-sustained-" + i)
                                    .doOnSuccess(clients::add)
                                    .onErrorResume(e -> Mono.empty()), 10))
                    .then(Mono.fromRunnable(() -> {
                        endTime.set(System.currentTimeMillis() + (durationSeconds * 1000L));
                    }))
                    .thenMany(
                        // 持续发送
                        Flux.fromIterable(clients)
                            .flatMap(conn -> {
                                AtomicInteger messageIdGen = new AtomicInteger(1);
                                return Flux.generate(sink -> {
                                               if (System.currentTimeMillis() < endTime.get()) {
                                                   sink.next(1);
                                               } else {
                                                   sink.complete();
                                               }
                                           })
                                           .flatMap(x -> publish(conn, topic, payload, MqttQoS.AT_MOST_ONCE, messageIdGen)
                                                   .doOnSuccess(v -> sentCount.incrementAndGet()), 128)
                                           .then();
                            }, clientCount))
                    .then(Mono.delay(Duration.ofSeconds(2)))
                    .then(Mono.fromRunnable(() -> {
                        System.out.println("=== 持续压力测试结果 ===");
                        System.out.println("持续时间: " + durationSeconds + " 秒");
                        System.out.println("客户端数: " + clientCount);
                        System.out.println("发送消息数: " + sentCount.get());
                        System.out.println("服务器接收数: " + receivedMessages.get());
                        System.out.println("平均吞吐量: " + (sentCount.get() / durationSeconds) + " 消息/秒");
                    }))
                    .thenMany(
                        // 断开连接
                        Flux.fromIterable(clients)
                            .flatMap(this::disconnect))
                    .then(Mono.fromRunnable(() -> System.out.println("MQTT 服务器已停止")))
        )
        .expectComplete()
        .verify(Duration.ofSeconds(durationSeconds + 10));
    }

    /**
     * 测试大消息
     */
    @Test
    void testLargeMessages() {
        int messageCount = 100;
        int messageSize = 32 * 1024;
        String topic = "stress/large";
        byte[] payload = new byte[messageSize];
        for (int i = 0; i < messageSize; i++) {
            payload[i] = (byte) (i % 256);
        }
        AtomicInteger messageIdGen = new AtomicInteger(1);
        AtomicLong startTime = new AtomicLong();

        StepVerifier.create(
            startServer()
                    .doOnSuccess(s -> startTime.set(System.currentTimeMillis()))
                    .flatMap(s -> createClient("stress-large")
                            .flatMap(conn ->
                                // 同步发送所有消息
                                Flux.range(0, messageCount)
                                    .flatMap(i -> publish(conn, topic, payload, MqttQoS.AT_LEAST_ONCE, messageIdGen), 16)
                                    .then()
                                    // 等待服务器处理
                                    .then(Mono.delay(Duration.ofSeconds(2)))
                                    .then(disconnect(conn))
                                    .thenReturn(conn)))
                    .flatMap(conn -> Mono.fromRunnable(() -> {
                        long elapsed = System.currentTimeMillis() - startTime.get();
                        double dataMB = (messageCount * messageSize) / (1024.0 * 1024.0);

                        System.out.println("=== 大消息测试结果 ===");
                        System.out.println("消息数: " + messageCount);
                        System.out.println("消息大小: " + (messageSize / 1024) + " KB");
                        System.out.println("总数据量: " + String.format("%.2f", dataMB) + " MB");
                        System.out.println("耗时: " + elapsed + " ms");
                        System.out.println("吞吐量: " + String.format("%.2f", dataMB * 1000 / elapsed) + " MB/秒");
                        System.out.println("接收消息数: " + receivedMessages.get());

                        assertEquals(messageCount, receivedMessages.get());
                        System.out.println("MQTT 服务器已停止");
                    }))
        )
        .expectComplete()
        .verify(TIMEOUT);
    }

    /**
     * 测试快速连接断开
     */
    @Test
    void testRapidConnectDisconnect() {
        int iterations = 50;
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);
        AtomicLong startTime = new AtomicLong();

        StepVerifier.create(
            startServer()
                    .doOnSuccess(s -> startTime.set(System.currentTimeMillis()))
                    .thenMany(
                        Flux.range(0, iterations)
                            .concatMap(i -> createClient("stress-rapid-" + i)
                                    .flatMap(conn -> disconnect(conn).thenReturn(true))
                                    .doOnSuccess(v -> successCount.incrementAndGet())
                                    .onErrorResume(e -> {
                                        failCount.incrementAndGet();
                                        System.err.println("第 " + i + " 次迭代失败: " + e.getMessage());
                                        return Mono.just(false);
                                    })))
                    .then(Mono.fromRunnable(() -> {
                        long elapsed = System.currentTimeMillis() - startTime.get();

                        System.out.println("=== 快速连接/断开测试结果 ===");
                        System.out.println("迭代次数: " + iterations);
                        System.out.println("成功: " + successCount.get());
                        System.out.println("失败: " + failCount.get());
                        System.out.println("耗时: " + elapsed + " ms");
                        System.out.println("速率: " + (iterations * 1000.0 / elapsed) + " 次/秒");

                        assertEquals(iterations, successCount.get());
                        System.out.println("MQTT 服务器已停止");
                    }))
        )
        .expectComplete()
        .verify(Duration.ofSeconds(60));
    }
}
