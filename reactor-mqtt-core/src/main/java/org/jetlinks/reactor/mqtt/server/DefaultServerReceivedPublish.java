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
import org.jetlinks.reactor.mqtt.ParsedTopic;
import reactor.core.publisher.Mono;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.function.Function;

import static org.jetlinks.reactor.mqtt.MqttConstants.Message.Header.PUBREC_HEADER;

/**
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
    private final String clientId;
    private final Function<MqttMessage, Mono<Void>> sender;
    @SuppressWarnings("unused") // accessed via VarHandle
    private volatile boolean acknowledged = false;
    @SuppressWarnings("unused") // accessed via VarHandle
    private volatile boolean released = false;

    // 新增：缓存解析后的主题层级
    private volatile String[] cachedTopicLevels;

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
    public String[] getTopicLevels() {
        // 使用局部变量减少 volatile 读取
        String[] levels = cachedTopicLevels;
        if (levels == null) {
            // 双检锁 + 零分配路径
            synchronized (this) {
                levels = cachedTopicLevels;
                if (levels == null) {
                    // 使用 ParsedTopic 的字符串去重池
                    levels = ParsedTopic.parse(getTopic()).getLevels();
                    cachedTopicLevels = levels;
                }
            }
        }
        return levels;
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
            if (!ACKNOWLEDGED.compareAndSet(this, false, true)) {
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
                        PUBREC_HEADER,
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
        if (RELEASED.compareAndSet(this, false, true)) {
            ReferenceCountUtil.safeRelease(message);
        }
    }
}
