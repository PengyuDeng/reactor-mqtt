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
import io.netty.handler.codec.mqtt.*;
import io.netty.util.ReferenceCountUtil;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 *
 * @author PengyuDeng
 */
public class DefaultServerReceivedPublish implements ServerReceivedPublish {

    private final MqttPublishMessage message;
    private final String clientId;
    private final Function<MqttMessage, Mono<Void>> sender;
    private final AtomicBoolean acknowledged = new AtomicBoolean(false);
    private final AtomicBoolean released = new AtomicBoolean(false);

    public DefaultServerReceivedPublish(MqttPublishMessage message, String clientId, Function<MqttMessage, Mono<Void>> sender) {
        this.message = message;
        this.clientId = clientId;
        this.sender = sender;
    }

    @Override
    public String getTopic() {
        return message.variableHeader().topicName();
    }

    @Override
    public String getClientId() {
        return clientId;
    }

    @Override
    public int getMessageId() {
        return message.variableHeader().packetId();
    }

    @Override
    public int getQosLevel() {
        return message.fixedHeader().qosLevel().value();
    }

    @Override
    public boolean isDup() {
        return message.fixedHeader().isDup();
    }

    @Override
    public boolean isRetain() {
        return message.fixedHeader().isRetain();
    }

    @Override
    public ByteBuf getPayload() {
        return message.payload();
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
            if (!acknowledged.compareAndSet(false, true)) {
                return Mono.empty();
            }

            MqttQoS qos = message.fixedHeader().qosLevel();
            Mono<Void> ackMono;

            if (qos == MqttQoS.AT_LEAST_ONCE) {
                MqttMessage pubAck = MqttMessageBuilders.pubAck()
                                                        .packetId(message.variableHeader().packetId())
                                                        .build();
                ackMono = sender.apply(pubAck);
            } else if (qos == MqttQoS.EXACTLY_ONCE) {
                MqttMessage pubRec = new MqttMessage(
                        new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 0),
                        MqttMessageIdVariableHeader.from(message.variableHeader().packetId()));
                ackMono = sender.apply(pubRec);
            } else {
                ackMono = Mono.empty();
            }

            return ackMono;
        });
    }

    /**
     * 释放消息资源
     */
    public void release() {
        if (released.compareAndSet(false, true)) {
            ReferenceCountUtil.safeRelease(message);
        }
    }
}
