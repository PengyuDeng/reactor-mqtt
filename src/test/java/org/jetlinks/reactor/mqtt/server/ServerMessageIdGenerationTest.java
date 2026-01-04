/*
 * Copyright 2025 JetLinks https://www.jetlinks.cn
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

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.*;
import org.jetlinks.reactor.mqtt.client.MqttClient;
import org.jetlinks.reactor.mqtt.client.ClientConnection;
import org.junit.jupiter.api.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.DisposableServer;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 测试 Server 端消息 ID 自动生成功能
 *
 * @author PengyuDeng
 */
class ServerMessageIdGenerationTest {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 21885;
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private DisposableServer server;
    private ClientConnection clientConnection;

    @AfterEach
    void tearDown() {
        if (clientConnection != null) {
            clientConnection.disconnect().block(Duration.ofSeconds(2));
        }
        if (server != null && !server.isDisposed()) {
            server.disposeNow();
        }
    }

    /**
     * 测试 Server 推送 QoS 1 消息时,如果 ID 为 0,自动生成消息 ID
     */
    @Test
    void testServerPublishQos1WithZeroIdGeneratesMessageId() {
        Sinks.One<Integer> messageIdSink = Sinks.one();
        Sinks.One<ServerConnection> connSink = Sinks.one();

        StepVerifier.create(
            // 启动 Server
            MqttServer.create()
                      .host(HOST)
                      .port(PORT)
                      .handle(connection -> {
                          connSink.tryEmitValue(connection);
                          return connection.listener(new NoOpListener()).accept();
                      })
                      .bind()
                      .doOnNext(s -> server = s)
                      .then(
                          // 创建客户端并连接
                          MqttClient.create()
                                    .host(HOST)
                                    .port(PORT)
                                    .clientId("test-client")
                                    .handlePublishing(pub -> {
                                        messageIdSink.tryEmitValue(pub.getMessageId());
                                        return pub.acknowledge();
                                    })
                                    .connect()
                                    .doOnNext(conn -> clientConnection = conn)
                                    .flatMap(conn -> connSink.asMono()
                                        .flatMap(serverConn -> {
                                            // Server 推送 QoS 1 消息,消息 ID 为 0
                                            MqttPublishMessage message = MqttMessageBuilders.publish()
                                                .topicName("test/topic")
                                                .payload(Unpooled.wrappedBuffer("hello from server".getBytes(StandardCharsets.UTF_8)))
                                                .qos(MqttQoS.AT_LEAST_ONCE)
                                                .messageId(0)  // ID 为 0,应该自动生成
                                                .build();
                                            return serverConn.publish(message);
                                        })
                                        .then(messageIdSink.asMono())
                                    )
                      )
        )
        .assertNext(messageId -> {
            assertTrue(messageId >= 1 && messageId <= 65535,
                       "消息 ID 应该在 1-65535 范围内,实际: " + messageId);
            System.out.println("✅ Server 自动生成的消息 ID: " + messageId);
        })
        .expectComplete()
        .verify(TIMEOUT);
    }

    /**
     * 测试 Server 推送 QoS 1 消息时,如果 ID 已有效,保持原 ID
     */
    @Test
    void testServerPublishQos1WithValidIdKeepsOriginalId() {
        final int EXPECTED_MESSAGE_ID = 12345;
        Sinks.One<Integer> messageIdSink = Sinks.one();
        Sinks.One<ServerConnection> connSink = Sinks.one();

        StepVerifier.create(
            // 启动 Server
            MqttServer.create()
                      .host(HOST)
                      .port(PORT)
                      .handle(connection -> {
                          connSink.tryEmitValue(connection);
                          return connection.listener(new NoOpListener()).accept();
                      })
                      .bind()
                      .doOnNext(s -> server = s)
                      .then(
                          // 创建客户端并连接
                          MqttClient.create()
                                    .host(HOST)
                                    .port(PORT)
                                    .clientId("test-client")
                                    .handlePublishing(pub -> {
                                        messageIdSink.tryEmitValue(pub.getMessageId());
                                        return pub.acknowledge();
                                    })
                                    .connect()
                                    .doOnNext(conn -> clientConnection = conn)
                                    .flatMap(conn -> connSink.asMono()
                                        .flatMap(serverConn -> {
                                            // Server 推送 QoS 1 消息,消息 ID 已设置
                                            MqttPublishMessage message = MqttMessageBuilders.publish()
                                                .topicName("test/topic")
                                                .payload(Unpooled.wrappedBuffer("hello from server".getBytes(StandardCharsets.UTF_8)))
                                                .qos(MqttQoS.AT_LEAST_ONCE)
                                                .messageId(EXPECTED_MESSAGE_ID)  // 已有有效 ID
                                                .build();
                                            return serverConn.publish(message);
                                        })
                                        .then(messageIdSink.asMono())
                                    )
                      )
        )
        .assertNext(messageId -> {
            assertEquals(EXPECTED_MESSAGE_ID, messageId,
                         "消息 ID 应该保持原值: " + EXPECTED_MESSAGE_ID);
            System.out.println("✅ Server 保持原消息 ID: " + messageId);
        })
        .expectComplete()
        .verify(TIMEOUT);
    }

    /**
     * 测试 Server 推送 QoS 0 消息,不需要消息 ID
     */
    @Test
    void testServerPublishQos0NoMessageId() {
        Sinks.One<ServerConnection> connSink = Sinks.one();
        Sinks.One<String> topicSink = Sinks.one();

        StepVerifier.create(
            // 启动 Server
            MqttServer.create()
                      .host(HOST)
                      .port(PORT)
                      .handle(connection -> {
                          connSink.tryEmitValue(connection);
                          return connection.listener(new NoOpListener()).accept();
                      })
                      .bind()
                      .doOnNext(s -> server = s)
                      .then(
                          // 创建客户端并连接
                          MqttClient.create()
                                    .host(HOST)
                                    .port(PORT)
                                    .clientId("test-client")
                                    .handlePublishing(pub -> {
                                        topicSink.tryEmitValue(pub.getTopic());
                                        return Mono.empty();
                                    })
                                    .connect()
                                    .doOnNext(conn -> clientConnection = conn)
                                    .flatMap(conn -> connSink.asMono()
                                        .flatMap(serverConn -> {
                                            // Server 推送 QoS 0 消息
                                            MqttPublishMessage message = MqttMessageBuilders.publish()
                                                .topicName("test/topic")
                                                .payload(Unpooled.wrappedBuffer("hello from server".getBytes(StandardCharsets.UTF_8)))
                                                .qos(MqttQoS.AT_MOST_ONCE)
                                                .build();
                                            return serverConn.publish(message);
                                        })
                                        .then(topicSink.asMono())
                                    )
                      )
        )
        .assertNext(topic -> {
            assertEquals("test/topic", topic);
            System.out.println("✅ QoS 0 消息成功接收");
        })
        .expectComplete()
        .verify(TIMEOUT);
    }

    /**
     * 测试 Server 连续推送多条消息,消息 ID 递增且唯一
     */
    @Test
    void testServerPublishMultipleMessagesWithIncrementingIds() {
        Sinks.One<ServerConnection> connSink = Sinks.one();
        Set<Integer> receivedIds = ConcurrentHashMap.newKeySet();
        AtomicInteger messageCount = new AtomicInteger(0);
        Sinks.One<Set<Integer>> completeSink = Sinks.one();

        StepVerifier.create(
            // 启动 Server
            MqttServer.create()
                      .host(HOST)
                      .port(PORT)
                      .handle(connection -> {
                          connSink.tryEmitValue(connection);
                          return connection.listener(new NoOpListener()).accept();
                      })
                      .bind()
                      .doOnNext(s -> server = s)
                      .then(
                          // 创建客户端并连接,然后推送 10 条消息
                          MqttClient.create()
                                    .host(HOST)
                                    .port(PORT)
                                    .clientId("test-client")
                                    .handlePublishing(pub -> {
                                        receivedIds.add(pub.getMessageId());
                                        if (messageCount.incrementAndGet() >= 10) {
                                            completeSink.tryEmitValue(receivedIds);
                                        }
                                        return pub.acknowledge();
                                    })
                                    .connect()
                                    .doOnNext(conn -> clientConnection = conn)
                                    .flatMap(conn -> connSink.asMono()
                                        .flatMapMany(serverConn ->
                                            // Server 连续推送 10 条消息,ID 都为 0
                                            Flux.range(0, 10)
                                                .flatMap(i -> {
                                                    MqttPublishMessage message = MqttMessageBuilders.publish()
                                                        .topicName("test/topic")
                                                        .payload(Unpooled.wrappedBuffer(("message-" + i).getBytes(StandardCharsets.UTF_8)))
                                                        .qos(MqttQoS.AT_LEAST_ONCE)
                                                        .messageId(0)  // ID 为 0,应该自动生成
                                                        .build();
                                                    return serverConn.publish(message);
                                                })
                                        )
                                        .then(completeSink.asMono())
                                    )
                      )
        )
        .assertNext(ids -> {
            assertEquals(10, ids.size(), "应该接收到 10 个唯一的消息 ID");
            for (int id : ids) {
                assertTrue(id >= 1 && id <= 65535,
                           "消息 ID 应该在 1-65535 范围内,实际: " + id);
            }
            System.out.println("✅ 接收到 10 个唯一的消息 ID: " + ids);
        })
        .expectComplete()
        .verify(TIMEOUT);
    }

    /**
     * 测试 QoS 2 消息 ID 生成
     */
    @Test
    void testServerPublishQos2WithZeroId() {
        Sinks.One<Integer> messageIdSink = Sinks.one();
        Sinks.One<ServerConnection> connSink = Sinks.one();

        StepVerifier.create(
            // 启动 Server
            MqttServer.create()
                      .host(HOST)
                      .port(PORT)
                      .handle(connection -> {
                          connSink.tryEmitValue(connection);
                          return connection.listener(new NoOpListener()).accept();
                      })
                      .bind()
                      .doOnNext(s -> server = s)
                      .then(
                          // 创建客户端并连接
                          MqttClient.create()
                                    .host(HOST)
                                    .port(PORT)
                                    .clientId("test-client")
                                    .handlePublishing(pub -> {
                                        messageIdSink.tryEmitValue(pub.getMessageId());
                                        return pub.acknowledge();
                                    })
                                    .connect()
                                    .doOnNext(conn -> clientConnection = conn)
                                    .flatMap(conn -> connSink.asMono()
                                        .flatMap(serverConn -> {
                                            // Server 推送 QoS 2 消息,消息 ID 为 0
                                            MqttPublishMessage message = MqttMessageBuilders.publish()
                                                .topicName("test/topic")
                                                .payload(Unpooled.wrappedBuffer("hello from server".getBytes(StandardCharsets.UTF_8)))
                                                .qos(MqttQoS.EXACTLY_ONCE)
                                                .messageId(0)  // ID 为 0,应该自动生成
                                                .build();
                                            return serverConn.publish(message);
                                        })
                                        .then(messageIdSink.asMono())
                                    )
                      )
        )
        .assertNext(messageId -> {
            assertTrue(messageId >= 1 && messageId <= 65535,
                       "消息 ID 应该在 1-65535 范围内,实际: " + messageId);
            System.out.println("✅ QoS 2 消息,Server 自动生成的消息 ID: " + messageId);
        })
        .expectComplete()
        .verify(TIMEOUT);
    }

    /**
     * 简单的空操作监听器
     */
    private static class NoOpListener implements MqttMessageListener {
        @Override
        public Mono<Void> onPublish(ServerReceivedPublish message) {
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
        public Mono<Void> onDisconnect(ServerConnection connection) {
            return Mono.empty();
        }
    }
}
