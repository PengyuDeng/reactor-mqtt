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
package org.jetlinks.reactor.mqtt.server.messages;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;
import reactor.core.publisher.Mono;

/**
 * MQTT PUBLISH 消息
 * <p>
 * 代表客户端发送的 PUBLISH 消息，包含主题、负载、QoS 等信息。
 * </p>
 *
 * @author PengyuDeng
 */
public interface MqttPublishMessage extends MqttMessage {

    /**
     * 获取消息主题
     *
     * @return 主题名称
     */
    String topicName();

    /**
     * 获取消息负载
     *
     * @return 消息内容（ByteBuf）
     */
    ByteBuf payload();

    /**
     * 获取 QoS 级别
     *
     * @return QoS 等级
     */
    MqttQoS qosLevel();

    /**
     * 是否为重复消息
     *
     * @return true 如果是重发的消息
     */
    boolean isDup();

    /**
     * 是否需要保留
     *
     * @return true 如果消息需要被 Broker 保留
     */
    boolean isRetain();

    /**
     * 获取 MQTT 5.0 属性
     *
     * @return MQTT 属性
     */
    @Override
    MqttProperties properties();

    /**
     * 确认消息（发送 ACK）
     * <p>
     * 如果 autoAck 关闭，需要手动调用此方法：
     * <ul>
     *   <li>QoS 0: 无操作</li>
     *   <li>QoS 1: 发送 PUBACK</li>
     *   <li>QoS 2: 发送 PUBREC</li>
     * </ul>
     * </p>
     *
     * @return 完成的 Mono
     */
    Mono<Void> ack();
}
