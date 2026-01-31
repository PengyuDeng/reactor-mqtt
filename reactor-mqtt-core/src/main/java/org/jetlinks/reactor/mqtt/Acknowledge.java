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
import reactor.core.publisher.Mono;

/**
 * 消息确认接口
 *
 * @author PengyuDeng
 */
public interface Acknowledge {

    /**
     * 确认消息（发送 ACK）
     *
     * @return 确认完成的 Mono
     */
    Mono<Void> ack();

    /**
     * 否定应答（发送 NACK）
     * <p>
     * 用于 MQTT 5.0，可以携带错误原因码和属性。
     * </p>
     *
     * @param properties MQTT 5.0 属性，可包含原因码和原因字符串
     * @return 完成的 Mono
     */
    Mono<Void> nack(MqttProperties properties);

    /**
     * 否定应答（发送 NACK），不带属性
     *
     * @return 完成的 Mono
     */
    default Mono<Void> nack() {
        return nack(MqttProperties.NO_PROPERTIES);
    }
}
