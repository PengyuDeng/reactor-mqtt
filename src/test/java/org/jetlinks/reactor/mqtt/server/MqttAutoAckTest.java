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

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.jetlinks.reactor.mqtt.client.MqttClient;
import org.jetlinks.reactor.mqtt.client.MqttClientConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.DisposableServer;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 测试 autoAck 功能
 *
 * @author PengyuDeng
 */
class MqttAutoAckTest {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 21885;  // 使用不同端口避免冲突
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private DisposableServer server;
    private MqttClientConnection clientConnection;

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
     * 测试自动应答模式（默认）
     * QoS1 消息处理完成后自动发送 PUBACK
     */
    @Test
    void testAutoAckDefault() {
        Sinks.One<String> payloadSink = Sinks.one();

        StepVerifier.create(
            MqttServer.create()
                      .host(HOST)
                      .port(PORT)
                      .handle(connection -> connection
                              // 默认 autoAck = true
                              .listener(new MqttMessageListener() {
                                  @Override
                                  public Mono<Void> onPublish(MqttPublishing message) {
                                      payloadSink.tryEmitValue(message.getPayload().toString(StandardCharsets.UTF_8));
                                      // 不调用 acknowledge()，依赖自动应答
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
                                  public Mono<Void> onDisconnect(MqttConnection connection) {
                                      return Mono.empty();
                                  }
                              }).accept())
                      .bind()
                      .doOnNext(s -> server = s)
                      .then(MqttClient.create()
                                      .host(HOST)
                                      .port(PORT)
                                      .clientId("auto-ack-client")
                                      .connect()
                                      .doOnNext(conn -> clientConnection = conn)
                                      .flatMap(conn ->
                                          // 发送 QoS1 消息，如果自动应答正常，客户端不会超时
                                          conn.publish("/test/auto-ack",
                                                  Unpooled.wrappedBuffer("auto-ack-message".getBytes(StandardCharsets.UTF_8)),
                                                  MqttQoS.AT_LEAST_ONCE)
                                              .then(payloadSink.asMono())
                                      ))
        )
        .assertNext(payload -> assertEquals("auto-ack-message", payload))
        .expectComplete()
        .verify(TIMEOUT);
    }

    /**
     * 测试显式开启自动应答
     */
    @Test
    void testAutoAckExplicitTrue() {
        AtomicInteger messageCount = new AtomicInteger(0);
        Sinks.One<Integer> completeSink = Sinks.one();

        StepVerifier.create(
            MqttServer.create()
                      .host(HOST)
                      .port(PORT)
                      .handle(connection -> connection
                              .autoAck(true)  // 显式设置自动应答
                              .listener(new MqttMessageListener() {
                                  @Override
                                  public Mono<Void> onPublish(MqttPublishing message) {
                                      int count = messageCount.incrementAndGet();
                                      if (count >= 3) {
                                          completeSink.tryEmitValue(count);
                                      }
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
                                  public Mono<Void> onDisconnect(MqttConnection connection) {
                                      return Mono.empty();
                                  }
                              }).accept())
                      .bind()
                      .doOnNext(s -> server = s)
                      .then(MqttClient.create()
                                      .host(HOST)
                                      .port(PORT)
                                      .clientId("auto-ack-explicit-client")
                                      .connect()
                                      .doOnNext(conn -> clientConnection = conn)
                                      .flatMap(conn ->
                                          // 发送多条 QoS1 消息
                                          Flux.range(0, 3)
                                              .flatMap(i -> conn.publish("/test/auto-ack",
                                                      Unpooled.wrappedBuffer(("message-" + i).getBytes(StandardCharsets.UTF_8)),
                                                      MqttQoS.AT_LEAST_ONCE))
                                              .then(completeSink.asMono())
                                      ))
        )
        .assertNext(count -> assertEquals(3, count))
        .expectComplete()
        .verify(TIMEOUT);
    }

    /**
     * 测试手动应答模式
     * 处理者需要自己调用 acknowledge()
     */
    @Test
    void testManualAck() {
        Sinks.One<String> payloadSink = Sinks.one();

        StepVerifier.create(
            MqttServer.create()
                      .host(HOST)
                      .port(PORT)
                      .handle(connection -> connection
                              .autoAck(false)  // 手动应答模式
                              .listener(new MqttMessageListener() {
                                  @Override
                                  public Mono<Void> onPublish(MqttPublishing message) {
                                      payloadSink.tryEmitValue(message.getPayload().toString(StandardCharsets.UTF_8));
                                      // 手动应答：模拟处理后再确认
                                      return Mono.delay(Duration.ofMillis(100))
                                                 .then(message.acknowledge());
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
                                  public Mono<Void> onDisconnect(MqttConnection connection) {
                                      return Mono.empty();
                                  }
                              }).accept())
                      .bind()
                      .doOnNext(s -> server = s)
                      .then(MqttClient.create()
                                      .host(HOST)
                                      .port(PORT)
                                      .clientId("manual-ack-client")
                                      .connect()
                                      .doOnNext(conn -> clientConnection = conn)
                                      .flatMap(conn ->
                                          // 发送 QoS1 消息
                                          conn.publish("/test/manual-ack",
                                                  Unpooled.wrappedBuffer("manual-ack-message".getBytes(StandardCharsets.UTF_8)),
                                                  MqttQoS.AT_LEAST_ONCE)
                                              .then(payloadSink.asMono())
                                      ))
        )
        .assertNext(payload -> assertEquals("manual-ack-message", payload))
        .expectComplete()
        .verify(TIMEOUT);
    }

    /**
     * 测试手动应答模式下的 QoS2 消息
     */
    @Test
    void testManualAckQoS2() {
        Sinks.One<String> payloadSink = Sinks.one();
        Sinks.One<Integer> qosSink = Sinks.one();

        StepVerifier.create(
            MqttServer.create()
                      .host(HOST)
                      .port(PORT)
                      .handle(connection -> connection
                              .autoAck(false)
                              .listener(new MqttMessageListener() {
                                  @Override
                                  public Mono<Void> onPublish(MqttPublishing message) {
                                      payloadSink.tryEmitValue(message.getPayload().toString(StandardCharsets.UTF_8));
                                      qosSink.tryEmitValue(message.getQosLevel());
                                      // 手动应答 QoS2
                                      return message.acknowledge();
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
                                  public Mono<Void> onDisconnect(MqttConnection connection) {
                                      return Mono.empty();
                                  }
                              }).accept())
                      .bind()
                      .doOnNext(s -> server = s)
                      .then(MqttClient.create()
                                      .host(HOST)
                                      .port(PORT)
                                      .clientId("manual-ack-qos2-client")
                                      .connect()
                                      .doOnNext(conn -> clientConnection = conn)
                                      .flatMap(conn ->
                                          // 发送 QoS2 消息
                                          conn.publish("/test/manual-ack-qos2",
                                                  Unpooled.wrappedBuffer("qos2-message".getBytes(StandardCharsets.UTF_8)),
                                                  MqttQoS.EXACTLY_ONCE)
                                              .then(Mono.zip(payloadSink.asMono(), qosSink.asMono()))
                                      ))
        )
        .assertNext(tuple -> {
            assertEquals("qos2-message", tuple.getT1());
            assertEquals(2, tuple.getT2());
        })
        .expectComplete()
        .verify(TIMEOUT);
    }

    /**
     * 测试 QoS0 消息不受 autoAck 影响（QoS0 无需应答）
     */
    @Test
    void testQoS0NotAffectedByAutoAck() {
        AtomicInteger messageCount = new AtomicInteger(0);
        Sinks.One<Integer> completeSink = Sinks.one();

        StepVerifier.create(
            MqttServer.create()
                      .host(HOST)
                      .port(PORT)
                      .handle(connection -> connection
                              .autoAck(false)  // 即使设置手动应答，QoS0 也不需要
                              .listener(new MqttMessageListener() {
                                  @Override
                                  public Mono<Void> onPublish(MqttPublishing message) {
                                      int count = messageCount.incrementAndGet();
                                      if (count >= 2) {
                                          completeSink.tryEmitValue(count);
                                      }
                                      // QoS0 不需要调用 acknowledge()
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
                                  public Mono<Void> onDisconnect(MqttConnection connection) {
                                      return Mono.empty();
                                  }
                              }).accept())
                      .bind()
                      .doOnNext(s -> server = s)
                      .then(MqttClient.create()
                                      .host(HOST)
                                      .port(PORT)
                                      .clientId("qos0-client")
                                      .connect()
                                      .doOnNext(conn -> clientConnection = conn)
                                      .flatMap(conn ->
                                          // 发送 QoS0 消息
                                          Flux.just("message1", "message2")
                                              .flatMap(msg -> conn.publish("/test/qos0",
                                                      Unpooled.wrappedBuffer(msg.getBytes(StandardCharsets.UTF_8)),
                                                      MqttQoS.AT_MOST_ONCE))
                                              .then(completeSink.asMono())
                                      ))
        )
        .assertNext(count -> assertEquals(2, count))
        .expectComplete()
        .verify(TIMEOUT);
    }

    /**
     * 测试手动应答模式下多条消息的顺序处理
     */
    @Test
    void testManualAckMultipleMessages() {
        AtomicInteger messageCount = new AtomicInteger(0);
        int expectedCount = 10;
        Sinks.One<Integer> completeSink = Sinks.one();

        StepVerifier.create(
            MqttServer.create()
                      .host(HOST)
                      .port(PORT)
                      .handle(connection -> connection
                              .autoAck(false)
                              .listener(new MqttMessageListener() {
                                  @Override
                                  public Mono<Void> onPublish(MqttPublishing message) {
                                      int count = messageCount.incrementAndGet();
                                      if (count >= expectedCount) {
                                          completeSink.tryEmitValue(count);
                                      }
                                      // 每条消息都手动应答
                                      return message.acknowledge();
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
                                  public Mono<Void> onDisconnect(MqttConnection connection) {
                                      return Mono.empty();
                                  }
                              }).accept())
                      .bind()
                      .doOnNext(s -> server = s)
                      .then(MqttClient.create()
                                      .host(HOST)
                                      .port(PORT)
                                      .clientId("multi-msg-client")
                                      .connect()
                                      .doOnNext(conn -> clientConnection = conn)
                                      .flatMap(conn ->
                                          // 发送多条 QoS1 消息
                                          Flux.range(0, expectedCount)
                                              .flatMap(i -> conn.publish("/test/multi",
                                                      Unpooled.wrappedBuffer(("msg-" + i).getBytes(StandardCharsets.UTF_8)),
                                                      MqttQoS.AT_LEAST_ONCE))
                                              .then(completeSink.asMono())
                                      ))
        )
        .assertNext(count -> assertEquals(expectedCount, count))
        .expectComplete()
        .verify(Duration.ofSeconds(10));
    }
}
