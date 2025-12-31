/*
 * Copyright 2025 JetLinks https://www.jetlinks.cn
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

import reactor.core.publisher.Mono;

/**
 * MQTT 消息监听器接口 - 纯响应式 API
 *
 * <p>提供统一的响应式消息处理接口，用于处理 MQTT 协议中的各类消息事件。
 * 所有方法都返回 {@link Mono}，支持响应式流处理。</p>
 *
 * <h3>使用示例：</h3>
 * <pre>{@code
 * MqttServer.create()
 *     .handle(connection -> {
 *         connection.listener(new MqttMessageListener() {
 *             @Override
 *             public Mono<Void> onPublish(MqttPublishing message) {
 *                 System.out.println("Received: " + message.getTopic());
 *                 return Mono.empty();
 *             }
 *
 *             @Override
 *             public Mono<Void> onSubscribe(MqttSubscription subscription) {
 *                 System.out.println("Subscribe: " + subscription.getMessage());
 *                 return Mono.empty();
 *             }
 *
 *             @Override
 *             public Mono<Void> onUnsubscribe(MqttUnSubscription unsubscription) {
 *                 System.out.println("Unsubscribe: " + unsubscription.getMessage());
 *                 return Mono.empty();
 *             }
 *
 *             @Override
 *             public Mono<Void> onDisconnect(MqttConnection connection) {
 *                 System.out.println("Disconnected: " + connection.getClientId());
 *                 return Mono.empty();
 *             }
 *         });
 *
 *         return connection.accept();
 *     })
 *     .bindNow();
 * }</pre>
 *
 * @author PengyuDeng
 * @see MqttConnection#listener(MqttMessageListener)
 */
public interface MqttMessageListener {

    /**
     * 处理客户端发布的消息
     *
     * <p>当客户端发布消息到服务器时调用此方法。
     * 对于 QoS 1/2 消息，ACK 会在此方法返回的 Mono 完成后自动发送。</p>
     *
     * @param message 发布的消息
     * @return 处理完成的 Mono
     */
    Mono<Void> onPublish(MqttPublishing message);

    /**
     * 处理客户端的订阅请求
     *
     * <p>当客户端发送订阅请求时调用此方法。
     * SUBACK 会在此方法返回的 Mono 完成后自动发送。</p>
     *
     * @param subscription 订阅请求
     * @return 处理完成的 Mono
     */
    Mono<Void> onSubscribe(MqttSubscription subscription);

    /**
     * 处理客户端的取消订阅请求
     *
     * <p>当客户端发送取消订阅请求时调用此方法。
     * UNSUBACK 会在此方法返回的 Mono 完成后自动发送。</p>
     *
     * @param unsubscription 取消订阅请求
     * @return 处理完成的 Mono
     */
    Mono<Void> onUnsubscribe(MqttUnSubscription unsubscription);

    /**
     * 处理连接关闭事件
     *
     * <p>当客户端断开连接时调用此方法。
     * 可用于清理资源或记录日志。</p>
     *
     * @param connection 断开的连接
     * @return 处理完成的 Mono
     */
    Mono<Void> onDisconnect(MqttConnection connection);
}
