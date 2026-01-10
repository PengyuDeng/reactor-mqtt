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
package org.jetlinks.reactor.mqtt.broker;

import org.jetlinks.reactor.mqtt.server.MqttServer;

/**
 * MQTT Broker 接口
 *
 * <p>继承自 {@link MqttServer}，在 Server 的基础上增加了消息路由功能，
 * 可以将客户端发布的消息转发给订阅了相应主题的其他客户端。</p>
 *
 * @author PengyuDeng
 * @since 1.0.0
 */
public interface MqttBroker extends MqttServer {

    /**
     * 创建一个新的 Broker 构建器
     *
     * @return Broker 构建器
     */
    static MqttBroker create() {
        return new DefaultMqttBroker();
    }


    // Broker 特有的方法

    /**
     * 获取当前连接的客户端数量
     *
     * @return 连接数
     */
    int getConnectionCount();

    /**
     * 获取当前订阅数量
     *
     * @return 订阅数
     */
    int getSubscriptionCount();

}
