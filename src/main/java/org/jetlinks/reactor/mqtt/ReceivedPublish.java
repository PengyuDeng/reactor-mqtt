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
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import reactor.core.publisher.Mono;

/**
 * MQTT 收到的 PUBLISH 消息
 *
 * <p>客户端和服务端共用的接收消息抽象。</p>
 *
 * @author PengyuDeng
 */
public interface ReceivedPublish extends Acknowledge {

    /**
     * 获取消息主题
     *
     * @return 主题名称
     */
    String getTopic();

    /**
     * 获取消息负载
     *
     * @return 负载数据
     */
    ByteBuf getPayload();

    /**
     * 获取负载的字节数组副本
     *
     * @return 负载字节数组
     */
    default byte[] getPayloadAsBytes() {
        ByteBuf payload = getPayload();
        if (payload == null || payload.readableBytes() == 0) {
            return new byte[0];
        }
        byte[] bytes = new byte[payload.readableBytes()];
        payload.getBytes(payload.readerIndex(), bytes);
        return bytes;
    }

    /**
     * 获取消息 ID
     *
     * @return 消息 ID（QoS 0 时为 0）
     */
    int getMessageId();

    /**
     * 获取 QoS 级别
     *
     * @return QoS 级别（0, 1, 2）
     */
    int getQosLevel();

    /**
     * 是否为重复消息
     *
     * @return true 如果是重复消息
     */
    boolean isDup();

    /**
     * 是否为保留消息
     *
     * @return true 如果是保留消息
     */
    boolean isRetain();

    /**
     * 获取 MQTT 5.0 属性
     *
     * @return MQTT 属性
     */
    MqttProperties getProperties();

    /**
     * 获取原始 MQTT 消息
     *
     * @return 原始消息对象
     */
    MqttPublishMessage getOrigin();
}
