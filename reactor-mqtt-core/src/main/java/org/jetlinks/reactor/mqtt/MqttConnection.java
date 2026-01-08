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

import io.netty.handler.codec.mqtt.MqttPublishMessage;
import reactor.core.publisher.Mono;

/**
 * MQTT 连接基础接口
 *
 * <p>客户端和服务端连接的公共抽象，定义通用的连接管理方法。</p>
 *
 * @author PengyuDeng
 */
public interface MqttConnection {

    /**
     * 获取客户端 ID
     *
     * @return 客户端 ID
     */
    String getClientId();

    /**
     * 连接是否存活
     *
     * @return true 如果连接存活
     */
    boolean isAlive();

    /**
     * 连接关闭事件
     *
     * @return 关闭时完成的 Mono
     */
    Mono<Void> onClose();

    /**
     * 关闭连接
     *
     * @return 关闭完成的 Mono
     */
    Mono<Void> close();

    /**
     * 发布消息
     *
     * @param message MQTT 发布消息
     * @return 发布完成的 Mono
     */
    Mono<Void> publish(MqttPublishMessage message);
}
