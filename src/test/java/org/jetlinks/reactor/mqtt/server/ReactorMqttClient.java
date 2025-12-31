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
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.Connection;
import reactor.netty.NettyOutbound;
import reactor.netty.tcp.TcpClient;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 基于 reactor-netty 的 MQTT 客户端
 * 用于测试目的
 *
 * @author PengyuDeng
 */
public class ReactorMqttClient {

    private static final int MAX_MESSAGE_SIZE = 65536;
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(10);

    private final String host;
    private final int port;
    private final String clientId;
    private final AtomicInteger messageIdGenerator = new AtomicInteger(1);

    private volatile Connection connection;
    private volatile NettyOutbound outbound;
    private volatile Sinks.Many<MqttMessage> inboundSink;

    public ReactorMqttClient(String host, int port, String clientId) {
        this.host = host;
        this.port = port;
        this.clientId = clientId;
    }

    /**
     * 连接到 MQTT 服务器
     */
    public void connect() {
        connect(DEFAULT_TIMEOUT);
    }

    /**
     * 连接到 MQTT 服务器
     *
     * @param timeout 连接超时时间
     */
    public void connect(Duration timeout) {
        this.inboundSink = Sinks.many().multicast().onBackpressureBuffer();

        CountDownLatch handlerReady = new CountDownLatch(1);

        this.connection = TcpClient.create()
                                   .host(host)
                                   .port(port)
                                   .doOnConnected(conn -> {
                                       conn.addHandlerLast("mqtt-decoder", new MqttDecoder(MAX_MESSAGE_SIZE));
                                       conn.addHandlerLast("mqtt-encoder", MqttEncoder.INSTANCE);
                                   })
                                   .handle((inbound, out) -> {
                                       this.outbound = out;

                                       // 订阅入站消息
                                       inbound.receiveObject()
                                              .cast(MqttMessage.class)
                                              .subscribe(
                                                      msg -> inboundSink.tryEmitNext(msg),
                                                      err -> inboundSink.tryEmitError(err),
                                                      () -> inboundSink.tryEmitComplete()
                                              );

                                       handlerReady.countDown();

                                       // 保持连接不关闭
                                       return Mono.never();
                                   })
                                   .connectNow(timeout);

        // 等待 handler 准备就绪
        try {
            if (!handlerReady.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new RuntimeException("等待连接处理器超时");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("等待连接处理器被中断", e);
        }

        // 发送 CONNECT 消息
        MqttConnectMessage connectMessage = MqttMessageBuilders.connect()
                                                               .clientId(clientId)
                                                               .cleanSession(true)
                                                               .keepAlive(300)
                                                               .build();

        outbound.sendObject(Mono.just(connectMessage))
                .then()
                .block(timeout);

        // 等待 CONNACK
        MqttMessage connAck = inboundSink.asFlux()
                                         .filter(msg -> msg.fixedHeader().messageType() == MqttMessageType.CONNACK)
                                         .next()
                                         .block(timeout);

        if (connAck == null) {
            throw new RuntimeException("未收到 CONNACK 响应");
        }

        MqttConnAckMessage ackMessage = (MqttConnAckMessage) connAck;
        if (ackMessage.variableHeader().connectReturnCode() != MqttConnectReturnCode.CONNECTION_ACCEPTED) {
            throw new RuntimeException("连接被拒绝: " + ackMessage.variableHeader().connectReturnCode());
        }
    }

    /**
     * 发布消息
     *
     * @param topic   主题
     * @param payload 消息内容
     * @param qos     QoS 级别
     * @param retain  是否保留
     */
    public void publish(String topic, byte[] payload, int qos, boolean retain) {
        publish(topic, payload, qos, retain, DEFAULT_TIMEOUT);
    }

    /**
     * 发布消息
     *
     * @param topic   主题
     * @param payload 消息内容
     * @param qos     QoS 级别
     * @param retain  是否保留
     * @param timeout 超时时间
     */
    public void publish(String topic, byte[] payload, int qos, boolean retain, Duration timeout) {
        if (connection == null || connection.isDisposed()) {
            throw new IllegalStateException("客户端未连接");
        }

        MqttQoS mqttQos = MqttQoS.valueOf(qos);
        int messageId = mqttQos == MqttQoS.AT_MOST_ONCE ? 0 : nextMessageId();

        ByteBuf payloadBuf = Unpooled.wrappedBuffer(payload);

        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.PUBLISH,
                false,
                mqttQos,
                retain,
                0
        );

        MqttPublishVariableHeader variableHeader = new MqttPublishVariableHeader(
                topic,
                messageId
        );

        MqttPublishMessage publishMessage = new MqttPublishMessage(fixedHeader, variableHeader, payloadBuf);

        outbound.sendObject(Mono.just(publishMessage))
                .then()
                .block(timeout);

        // QoS1 等待 PUBACK，QoS2 等待完整的握手流程
        if (qos == 1) {
            waitForPubAck(messageId, timeout);
        } else if (qos == 2) {
            waitForQoS2Handshake(messageId, timeout);
        }
    }

    /**
     * 等待 PUBACK 响应
     */
    private void waitForPubAck(int messageId, Duration timeout) {
        inboundSink.asFlux()
                   .filter(msg -> msg.fixedHeader().messageType() == MqttMessageType.PUBACK)
                   .cast(MqttPubAckMessage.class)
                   .filter(msg -> msg.variableHeader().messageId() == messageId)
                   .next()
                   .block(timeout);
    }

    /**
     * 等待 QoS2 握手完成 (PUBREC -> PUBREL -> PUBCOMP)
     */
    private void waitForQoS2Handshake(int messageId, Duration timeout) {
        // 等待 PUBREC
        inboundSink.asFlux()
                   .filter(msg -> msg.fixedHeader().messageType() == MqttMessageType.PUBREC)
                   .filter(msg -> {
                       MqttMessageIdVariableHeader header = (MqttMessageIdVariableHeader) msg.variableHeader();
                       return header.messageId() == messageId;
                   })
                   .next()
                   .block(timeout);

        // 发送 PUBREL
        MqttFixedHeader pubRelHeader = new MqttFixedHeader(
                MqttMessageType.PUBREL,
                false,
                MqttQoS.AT_LEAST_ONCE,
                false,
                0
        );
        MqttPubReplyMessageVariableHeader pubRelVariableHeader =
                new MqttPubReplyMessageVariableHeader(messageId, (byte) 0, MqttProperties.NO_PROPERTIES);
        MqttMessage pubRelMessage = new MqttMessage(pubRelHeader, pubRelVariableHeader);

        outbound.sendObject(Mono.just(pubRelMessage))
                .then()
                .block(timeout);

        // 等待 PUBCOMP
        inboundSink.asFlux()
                   .filter(msg -> msg.fixedHeader().messageType() == MqttMessageType.PUBCOMP)
                   .filter(msg -> {
                       MqttMessageIdVariableHeader header = (MqttMessageIdVariableHeader) msg.variableHeader();
                       return header.messageId() == messageId;
                   })
                   .next()
                   .block(timeout);
    }

    /**
     * 断开连接
     */
    public void disconnect() {
        if (connection != null && !connection.isDisposed()) {
            // 发送 DISCONNECT 消息
            MqttFixedHeader fixedHeader = new MqttFixedHeader(
                    MqttMessageType.DISCONNECT,
                    false,
                    MqttQoS.AT_MOST_ONCE,
                    false,
                    0
            );
            MqttMessage disconnectMessage = new MqttMessage(fixedHeader);

            try {
                outbound.sendObject(Mono.just(disconnectMessage))
                        .then()
                        .block(Duration.ofSeconds(1));
            } catch (Exception ignored) {
                // 忽略断开连接时的异常
            }

            connection.dispose();
        }
    }

    /**
     * 生成下一个消息 ID
     */
    private int nextMessageId() {
        int id = messageIdGenerator.getAndIncrement() & 0xFFFF;
        if (id == 0) {
            id = messageIdGenerator.getAndIncrement() & 0xFFFF;
        }
        return id;
    }

    /**
     * 检查是否已连接
     */
    public boolean isConnected() {
        return connection != null && !connection.isDisposed();
    }
}
