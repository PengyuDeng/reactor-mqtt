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

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.*;
import io.netty.util.ReferenceCountUtil;
import reactor.core.publisher.Mono;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * MQTT 客户端接收消息实现
 *
 * @author PengyuDeng
 */
public class DefaultClientReceivedPublish implements ClientReceivedPublish {

    private final MqttPublishMessage message;
    private final DefaultClientConnection connection;
    private volatile boolean acknowledged = false;
    private volatile boolean released = false;

    private static final VarHandle ACKNOWLEDGED;
    private static final VarHandle RELEASED;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            ACKNOWLEDGED = lookup.findVarHandle(DefaultClientReceivedPublish.class, "acknowledged", boolean.class);
            RELEASED = lookup.findVarHandle(DefaultClientReceivedPublish.class, "released", boolean.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public DefaultClientReceivedPublish(MqttPublishMessage message, DefaultClientConnection connection) {
        this.message = message;
        this.connection = connection;
    }

    @Override
    public String getTopic() {
        return message.variableHeader().topicName();
    }

    @Override
    public ByteBuf getPayload() {
        return message.payload();
    }

    @Override
    public int getQosLevel() {
        return message.fixedHeader().qosLevel().value();
    }

    @Override
    public boolean isRetain() {
        return message.fixedHeader().isRetain();
    }

    @Override
    public boolean isDup() {
        return message.fixedHeader().isDup();
    }

    @Override
    public int getMessageId() {
        return message.variableHeader().packetId();
    }

    @Override
    public MqttProperties getProperties() {
        return message.variableHeader().properties();
    }

    @Override
    public MqttPublishMessage getOrigin() {
        return message;
    }

    @Override
    public Mono<Void> acknowledge() {
        return Mono.defer(() -> {
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
                        new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 0),
                        MqttMessageIdVariableHeader.from(message.variableHeader().packetId()));
                return connection.send(pubRec);
            }

            return Mono.empty();
        });
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
