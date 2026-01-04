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
package org.jetlinks.reactor.mqtt;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;

/**
 * MQTT 遗嘱（Last Will）消息
 *
 * @author PengyuDeng
 */
public class MqttWillMessage {

    private static final byte FLAG_RETAIN = 1;

    /**
     * 空遗嘱
     */
    public static final MqttWillMessage EMPTY = new MqttWillMessage(null, null, null, false, null);

    private final String topic;
    private final ByteBuf payload;
    private final MqttQoS qos;
    private final byte flags;
    private final MqttProperties properties;

    /**
     * 构造遗嘱消息
     *
     * @param topic      主题
     * @param payload    负载
     * @param qos        QoS 级别
     * @param retain     是否保留
     * @param properties MQTT 5.0 属性（可为 null）
     */
    public MqttWillMessage(String topic, ByteBuf payload, MqttQoS qos, boolean retain, MqttProperties properties) {
        this.topic = topic;
        this.payload = payload;
        this.qos = qos;
        this.flags = (byte) (retain ? FLAG_RETAIN : 0);
        this.properties = properties != null ? properties : MqttProperties.NO_PROPERTIES;
    }

    /**
     * 构造遗嘱消息（无 MQTT 5.0 属性）
     */
    public MqttWillMessage(String topic, ByteBuf payload, MqttQoS qos, boolean retain) {
        this(topic, payload, qos, retain, null);
    }

    /**
     * 构造遗嘱消息（使用字节数组）
     */
    public MqttWillMessage(String topic, byte[] payload, MqttQoS qos, boolean retain) {
        this(topic, payload != null ? Unpooled.wrappedBuffer(payload) : null, qos, retain, null);
    }

    /**
     * 是否有遗嘱
     */
    public boolean hasWill() {
        return topic != null;
    }

    /**
     * 获取主题
     */
    public String topic() {
        return topic;
    }

    /**
     * 获取负载
     */
    public ByteBuf payload() {
        return payload;
    }

    /**
     * 获取 QoS 级别
     */
    public MqttQoS qos() {
        return qos;
    }

    /**
     * 是否保留
     */
    public boolean retain() {
        return (flags & FLAG_RETAIN) != 0;
    }

    /**
     * 获取 MQTT 5.0 属性
     */
    public MqttProperties properties() {
        return properties;
    }

    /**
     * 获取负载字节数组
     */
    public byte[] payloadBytes() {
        if (payload == null || payload.readableBytes() == 0) {
            return null;
        }
        byte[] bytes = new byte[payload.readableBytes()];
        payload.getBytes(payload.readerIndex(), bytes);
        return bytes;
    }

    @Override
    public String toString() {
        if (!hasWill()) {
            return "MqttWill{none}";
        }
        return "MqttWill{" +
                "topic='" + topic + '\'' +
                ", qos=" + qos +
                ", retain=" + retain() +
                '}';
    }
}
