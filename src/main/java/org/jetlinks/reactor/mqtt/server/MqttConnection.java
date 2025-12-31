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

import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.time.Duration;

/**
 * MQTT 连接接口 - 纯响应式 API
 *
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
 *         });
 *
 *         return connection.accept();
 *     })
 *     .bindNow();
 * }</pre>
 *
 * @author PengyuDeng
 */
public interface MqttConnection {

    /**
     * 获取客户端 ID
     */
    String getClientId();

    /**
     * 获取认证信息
     */
    MqttAuth getAuth();

    /**
     * 拒绝连接
     */
    Mono<Void> reject(MqttConnectReturnCode code);

    /**
     * 接受连接
     */
    Mono<Void> accept();

    /**
     * 获取遗言消息
     */
    MqttWill getWill();

    /**
     * 设置响应式消息监听器
     * <p>
     * 使用统一的监听器接口处理所有 MQTT 消息事件。
     * 这是推荐的消息处理方式，所有回调都返回 {@link Mono}。
     * </p>
     *
     * <pre>{@code
     * connection.listener(new MqttMessageListener() {
     *     @Override
     *     public Mono<Void> onPublish(MqttPublishing message) {
     *         return processMessage(message)
     *             .then(message.acknowledge());
     *     }
     * });
     * }</pre>
     *
     * @param listener 消息监听器
     * @return 当前连接实例（支持链式调用）
     * @see MqttMessageListener
     */
    MqttConnection listener(MqttMessageListener listener);

    /**
     * 发布消息到客户端
     */
    Mono<Void> publish(MqttPublishMessage message);

    /**
     * 连接关闭事件
     */
    Mono<Void> onDispose();

    /**
     * 连接是否存活
     */
    boolean isAlive();

    /**
     * 关闭连接
     */
    Mono<Void> close();

    /**
     * 获取最后一次 ping 时间
     */
    long getLastPingTime();

    /**
     * 获取 keepAlive 超时时间
     */
    Duration getKeepAliveTimeout();

    /**
     * 设置 keepAlive 超时时间
     */
    Mono<Void> setKeepAliveTimeout(Duration duration);

    /**
     * 获取客户端地址
     */
    InetSocketAddress getClientAddress();
}
