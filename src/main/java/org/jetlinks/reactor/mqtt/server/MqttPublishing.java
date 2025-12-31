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
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import reactor.core.publisher.Mono;

/**
 * MQTT 发布消息
 *
 * @author PengyuDeng
 */
public interface MqttPublishing {

    /**
     * 获取消息主题
     */
    String getTopic();

    /**
     * 获取发送消息的客户端 ID
     */
    String getClientId();

    /**
     * 获取消息 ID
     */
    int getMessageId();

    /**
     * 获取 QoS 级别
     */
    int getQosLevel();

    /**
     * 是否为重复消息
     */
    boolean isDup();

    /**
     * 是否为保留消息
     */
    boolean isRetain();

    /**
     * 获取消息内容
     */
    ByteBuf getPayload();

    /**
     * 获取 MQTT 5.0 属性
     */
    MqttProperties getProperties();

    /**
     * 获取原始 MQTT 消息
     */
    MqttPublishMessage getMessage();

    /**
     * 确认消息（发送 ACK）
     * <p>
     * QoS 0: 无操作
     * QoS 1: 发送 PUBACK
     * QoS 2: 发送 PUBREC
     * </p>
     */
    Mono<Void> acknowledge();
}
