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

import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;

/**
 * MQTT 发布消息接口
 *
 * <p>封装接收到的 MQTT PUBLISH 消息，提供简洁的访问方法和应答机制。</p>
 *
 * @author PengyuDeng
 */
public interface MqttPublishing extends Acknowledge {

    /**
     * 获取消息主题
     *
     * @return 主题对象
     */
    Topic topic();

    /**
     * 获取原始 MQTT 发布消息
     *
     * @return 原始消息对象
     */
    MqttPublishMessage message();

    /**
     * 获取 MQTT 5.0 属性
     *
     * @return MQTT 属性
     */
    MqttProperties properties();
}
