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

/**
 * MQTT 消息基础接口
 *
 * @author PengyuDeng
 */
public interface MqttMessage {

    /**
     * 获取消息 ID
     *
     * @return 消息标识符（QoS 0 时为 0）
     */
    int messageId();

    /**
     * 获取 MQTT 5.0 属性
     *
     * @return MQTT 属性
     */
    default MqttProperties properties() {
        return MqttProperties.NO_PROPERTIES;
    }
}
