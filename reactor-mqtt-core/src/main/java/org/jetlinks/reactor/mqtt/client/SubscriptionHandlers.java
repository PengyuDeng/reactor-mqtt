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
 * 订阅处理器容器接口
 * <p>
 * 用于管理同一主题的多个消息处理器，支持以下功能：
 * <ul>
 *   <li>添加和移除消息处理器</li>
 *   <li>处理接收到的 MQTT 消息</li>
 *   <li>管理订阅生命周期（首次订阅和最后取消订阅）</li>
 *   <li>清理资源</li>
 * </ul>
 *
 * @author PengyuDeng
 */
interface SubscriptionHandlers {

    /**
     * 获取订阅主题
     *
     * @return 订阅主题
     */
    String getTopic();

    /**
     * 获取订阅的 QoS 级别
     *
     * @return QoS 级别
     */
    MqttQoS getQos();

    /**
     * 获取关联的连接
     *
     * @return 客户端连接
     */
    ClientConnection getConnection();

    /**
     * 添加消息处理器
     * <p>
     * 当第一个处理器被添加时，会触发实际的 MQTT SUBSCRIBE 操作。
     * 多个处理器可以订阅同一主题，消息会分发给所有处理器。
     *
     * @param handler 消息处理器函数
     * @return Disposable 用于移除此处理器，调用 dispose() 会移除该处理器，
     * 当最后一个处理器被移除时，会自动取消订阅
     */
    Disposable addHandler(Function<ClientReceivedPublish, Mono<Void>> handler, Runnable onLastRemoved);

    /**
     * 处理接收到的消息
     * <p>
     * 将消息分发给所有已注册的处理器。每个处理器的错误会被独立隔离，
     * 不会影响其他处理器的执行。
     *
     * @param publishing 接收到的发布消息
     * @return Mono 处理完成的信号，当所有处理器都处理完成时完成
     */
    Mono<Void> handle(ClientReceivedPublish publishing);

    /**
     * 清理资源并取消订阅
     * <p>
     * 如果连接仍然活跃，会发送 UNSUBSCRIBE 消息到服务器。
     * 清理所有已注册的处理器。
     */
    void dispose();

    /**
     * 检查是否已订阅
     *
     * @return true 如果已发送 SUBSCRIBE 到服务器，false 否则
     */
    boolean isSubscribed();
}
