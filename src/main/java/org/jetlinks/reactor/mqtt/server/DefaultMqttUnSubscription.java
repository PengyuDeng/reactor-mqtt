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

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * MQTT 取消订阅请求实现
 *
 * @author PengyuDeng
 */
public class DefaultMqttUnSubscription implements MqttUnSubscription {

    private final MqttUnsubscribeMessage message;
    private final Function<MqttMessage, Mono<Void>> sender;
    private final AtomicBoolean acknowledged = new AtomicBoolean(false);

    public DefaultMqttUnSubscription(MqttUnsubscribeMessage message, Function<MqttMessage, Mono<Void>> sender) {
        this.message = message;
        this.sender = sender;
    }

    @Override
    public MqttUnsubscribeMessage getMessage() {
        return message;
    }

    @Override
    public Mono<Void> acknowledge() {
        return Mono.defer(() -> {
            if (!acknowledged.compareAndSet(false, true)) {
                return Mono.empty();
            }

            MqttUnsubAckMessage unsubAck = MqttMessageBuilders.unsubAck()
                .packetId(message.variableHeader().messageId())
                .build();
            return sender.apply(unsubAck);
        });
    }
}
