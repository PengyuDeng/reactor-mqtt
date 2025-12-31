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

import io.netty.handler.codec.mqtt.MqttProperties;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * MQTT UNSUBSCRIBE 消息
 * <p>
 * 代表客户端发送的 UNSUBSCRIBE 消息，包含取消订阅的主题列表。
 * </p>
 *
 * @author PengyuDeng
 */
public interface MqttUnsubscribeMessage extends MqttMessage {

    /**
     * 获取取消订阅的主题列表
     *
     * @return 主题名称列表
     */
    List<String> topics();

    /**
     * 获取 MQTT 5.0 属性
     *
     * @return MQTT 属性
     */
    @Override
    MqttProperties properties();

    /**
     * 确认取消订阅（发送 UNSUBACK）
     *
     * @return 完成的 Mono
     */
    Mono<Void> ack();
}
