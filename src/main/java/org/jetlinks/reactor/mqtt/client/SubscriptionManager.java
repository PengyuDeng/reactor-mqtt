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

import io.netty.handler.codec.mqtt.MqttQoS;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * MQTT 订阅管理器
 *
 * <p>管理 MQTT 客户端的订阅，支持主题匹配和消息分发。</p>
 *
 * @author PengyuDeng
 */
public interface SubscriptionManager {

    /**
     * 订阅主题
     *
     * @param connection 客户端连接
     * @param topic      主题（支持通配符 + 和 #）
     * @param handler    消息处理器
     * @return Disposable 用于取消订阅
     */
    default Disposable subscribe(MqttClientConnection connection,
                                 CharSequence topic,
                                 Function<MqttClientPublishing, Mono<Void>> handler) {
        return subscribe(connection, topic, MqttQoS.AT_MOST_ONCE, handler);
    }

    /**
     * 订阅主题
     *
     * @param connection 客户端连接
     * @param topic      主题（支持通配符 + 和 #）
     * @param qos        QoS 级别
     * @param handler    消息处理器
     * @return Disposable 用于取消订阅
     */
    Disposable subscribe(MqttClientConnection connection,
                         CharSequence topic,
                         MqttQoS qos,
                         Function<MqttClientPublishing, Mono<Void>> handler);

    /**
     * 处理收到的消息，分发到匹配的订阅处理器
     *
     * @param publishing 收到的消息
     * @return 处理完成的 Mono
     */
    Mono<Void> handleMessage(MqttClientPublishing publishing);

    /**
     * 获取所有订阅的主题（用于重连后重新订阅）
     *
     * @return 订阅信息的可迭代对象
     */
    Iterable<SubscriptionInfo> getSubscriptions();

    /**
     * 清除所有订阅
     */
    void clear();

    /**
     * 订阅信息
     */
    interface SubscriptionInfo {
        /**
         * @return 订阅主题
         */
        String topic();

        /**
         * @return QoS 级别
         */
        MqttQoS qos();
    }

    /**
     * 创建默认的订阅管理器
     *
     * @return 订阅管理器实例
     */
    static SubscriptionManager create() {
        return new DefaultSubscriptionManager();
    }
}
