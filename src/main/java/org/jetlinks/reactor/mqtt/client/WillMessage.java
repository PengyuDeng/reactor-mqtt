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
import io.netty.handler.codec.mqtt.MqttQoS;

/**
 * MQTT 遗嘱消息
 *
 * @author PengyuDeng
 */
public class WillMessage {

    private static final byte FLAG_RETAIN = 1;

    private final String topic;
    private final ByteBuf payload;
    private final MqttQoS qos;
    private final byte flags;

    public WillMessage(String topic, ByteBuf payload, MqttQoS qos, boolean retain) {
        this.topic = topic;
        this.payload = payload;
        this.qos = qos;
        this.flags = (byte) (retain ? FLAG_RETAIN : 0);
    }

    public String topic() {
        return topic;
    }

    public ByteBuf payload() {
        return payload;
    }

    public MqttQoS qos() {
        return qos;
    }

    public boolean retain() {
        return (flags & FLAG_RETAIN) != 0;
    }

    /**
     * 获取 payload 字节数组
     */
    public byte[] payloadBytes() {
        if (payload == null || payload.readableBytes() == 0) {
            return null;
        }
        byte[] bytes = new byte[payload.readableBytes()];
        payload.getBytes(payload.readerIndex(), bytes);
        return bytes;
    }
}
