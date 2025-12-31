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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.NettyOutbound;
import reactor.netty.tcp.TcpClient;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * MQTT 客户端发送测试 - 纯响应式实现
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
    private static final byte[] PAYLOAD = "test-payload".getBytes(StandardCharsets.UTF_8);
    private static final int MAX_MESSAGE_SIZE = 65536;      // 最大消息大小
    // =============================================

    private static final LongAdder sentCount = new LongAdder();
    private static final LongAdder connectedClients = new LongAdder();
    private static final AtomicInteger messageIdGenerator = new AtomicInteger(1);

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("  MQTT 客户端发送测试 (响应式)");
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
        return TcpClient.create()
            .host(SERVER_HOST)
            .port(SERVER_PORT)
            .doOnConnected(conn -> {
                // 添加 MQTT 编解码器
                conn.addHandlerLast("mqtt-decoder", new MqttDecoder(MAX_MESSAGE_SIZE));
                conn.addHandlerLast("mqtt-encoder", MqttEncoder.INSTANCE);
            })
            .handle((inbound, outbound) -> {
                // 构建 CONNECT 消息
                MqttConnectMessage connectMessage = MqttMessageBuilders.connect()
                    .clientId(clientId)
                    .cleanSession(true)
                    .keepAlive(300)
                    .build();

                // 发送 CONNECT 并等待 CONNACK
                return outbound.sendObject(Mono.just(connectMessage))
                    .then(inbound.receiveObject()
                        .cast(MqttMessage.class)
                        .filter(msg -> msg.fixedHeader().messageType() == MqttMessageType.CONNACK)
                        .next()
                        .timeout(java.time.Duration.ofSeconds(10))
                        .flatMap(connAck -> {
                            MqttConnAckMessage ack = (MqttConnAckMessage) connAck;
                            if (ack.variableHeader().connectReturnCode() != MqttConnectReturnCode.CONNECTION_ACCEPTED) {
                                return Mono.error(new RuntimeException("连接被拒绝: " + ack.variableHeader().connectReturnCode()));
                            }
                            System.out.println("客户端连接成功: " + clientId);
                            connectedClients.increment();

                            // 开始发送消息
                            return sendMessages(outbound, clientId)
                                .doFinally(signal -> connectedClients.decrement());
                        }));
            })
            .connect()
            .doOnError(e -> System.out.println("客户端 " + clientId + " 连接失败: " + e.getMessage()))
            .flatMap(conn -> conn.onDispose())
            .onErrorResume(e -> Mono.empty());
    }

    /**
     * 在指定时间内持续发送消息
     */
    private static Mono<Void> sendMessages(NettyOutbound outbound, String clientId) {
        long endTime = System.currentTimeMillis() + (TEST_DURATION_SECONDS * 1000L);

        return Flux.generate(sink -> {
                if (System.currentTimeMillis() < endTime) {
                    sink.next(buildPublishMessage());
                } else {
                    sink.complete();
                }
            })
            .cast(MqttPublishMessage.class)
            .flatMap(msg -> outbound.sendObject(Mono.just(msg))
                .then(Mono.fromRunnable(sentCount::increment)), 256)
            .then();
    }

    /**
     * 构建 PUBLISH 消息
     */
    private static MqttPublishMessage buildPublishMessage() {
        int messageId = QOS == MqttQoS.AT_MOST_ONCE ? 0 : messageIdGenerator.getAndIncrement() & 0xFFFF;
        if (messageId == 0 && QOS != MqttQoS.AT_MOST_ONCE) {
            messageId = messageIdGenerator.getAndIncrement() & 0xFFFF;
        }

        ByteBuf payload = Unpooled.wrappedBuffer(PAYLOAD);

        MqttFixedHeader fixedHeader = new MqttFixedHeader(
            MqttMessageType.PUBLISH,
            false,
            QOS,
            false,
            0
        );

        MqttPublishVariableHeader variableHeader = new MqttPublishVariableHeader(
            TOPIC,
            messageId
        );

        return new MqttPublishMessage(fixedHeader, variableHeader, payload);
    }
}
