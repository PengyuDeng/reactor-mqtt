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

import io.netty.handler.codec.mqtt.*;
import io.netty.util.ReferenceCountUtil;
import org.jetlinks.reactor.mqtt.Topic;
import reactor.core.publisher.Mono;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import static org.jetlinks.reactor.mqtt.MqttConstants.MessageHeader.PUBREC_HEADER;

/**
 * 服务端接收消息实现
 *
 * @author PengyuDeng
 */
public class DefaultServerReceivedPublish implements ServerReceivedPublish {

    private static final VarHandle ACKNOWLEDGED;
    private static final VarHandle RELEASED;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            ACKNOWLEDGED = lookup.findVarHandle(DefaultServerReceivedPublish.class, "acknowledged", boolean.class);
            RELEASED = lookup.findVarHandle(DefaultServerReceivedPublish.class, "released", boolean.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final MqttPublishMessage message;
    private final Topic topic;
    private final DefaultServerConnection connection;

    @SuppressWarnings("unused")
    private volatile boolean acknowledged = false;
    @SuppressWarnings("unused")
    private volatile boolean released = false;

    DefaultServerReceivedPublish(MqttPublishMessage message, Topic topic, DefaultServerConnection connection) {
        this.message = message;
        this.topic = topic;
        this.connection = connection;
    }

    @Override
    public Topic topic() {
        return topic;
    }

    @Override
    public MqttPublishMessage message() {
        return message;
    }

    @Override
    public MqttProperties properties() {
        return message.variableHeader().properties();
    }

    @Override
    public String clientId() {
        return connection.getClientId();
    }

    @Override
    public Mono<Void> ack() {
        if (!ACKNOWLEDGED.compareAndSet(this, false, true)) {
            return Mono.empty();
        }

        MqttQoS qos = message.fixedHeader().qosLevel();

        if (qos == MqttQoS.AT_LEAST_ONCE) {
            MqttMessage pubAck = MqttMessageBuilders.pubAck()
                                                    .packetId(message.variableHeader().packetId())
                                                    .build();
            return connection.send(pubAck);
        } else if (qos == MqttQoS.EXACTLY_ONCE) {
            MqttMessage pubRec = new MqttMessage(
                    PUBREC_HEADER,
                    MqttMessageIdVariableHeader.from(message.variableHeader().packetId()));
            return connection.send(pubRec);
        }

        return Mono.empty();
    }

    @Override
    public Mono<Void> nack(MqttProperties properties) {
        if (!ACKNOWLEDGED.compareAndSet(this, false, true)) {
            return Mono.empty();
        }

        MqttQoS qos = message.fixedHeader().qosLevel();

        if (qos == MqttQoS.AT_LEAST_ONCE) {
            MqttMessage pubAck = MqttMessageBuilders.pubAck()
                                                    .packetId(message.variableHeader().packetId())
                                                    .reasonCode((byte) 0x80)
                                                    .properties(properties)
                                                    .build();
            return connection.send(pubAck);
        } else if (qos == MqttQoS.EXACTLY_ONCE) {
            MqttPubReplyMessageVariableHeader variableHeader = new MqttPubReplyMessageVariableHeader(
                    message.variableHeader().packetId(), (byte) 0x80, properties);
            MqttMessage pubRec = new MqttMessage(PUBREC_HEADER, variableHeader);
            return connection.send(pubRec);
        }

        return Mono.empty();
    }

    /**
     * 释放消息资源
     */
    public void release() {
        if (RELEASED.compareAndSet(this, false, true)) {
            ReferenceCountUtil.safeRelease(message);
        }
    }
}
