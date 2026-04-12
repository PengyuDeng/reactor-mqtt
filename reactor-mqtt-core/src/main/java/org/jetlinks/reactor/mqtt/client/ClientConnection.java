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

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.jetlinks.reactor.mqtt.MqttConnection;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

/**
 * MQTT 客户端连接接口
 *
 * <p>扩展公共连接接口，提供响应式 API 进行消息发布、订阅和连接管理。</p>
 *
 * <h3>使用示例：</h3>
 * <pre>{@code
 * MqttClient.create()
 *     .host("127.0.0.1")
 *     .port(1883)
 *     .connect()
 *     .flatMap(conn -> {
 *         // 订阅主题
 *         Disposable sub = conn
 *             .subscribe
 *             ("/topic", msg -> {
 *             System.out.println("Received: " + msg.getTopic());
 *             return Mono.empty();
 *         });
 *
 *         // 发布消息
 *         return conn.publish("/topic", payload, MqttQoS.AT_LEAST_ONCE)
 *                    .then(conn.onClose());
 *     })
 *     .block();
 * }</pre>
 *
 * @author PengyuDeng
 */
public interface ClientConnection extends MqttConnection {

    /**
     * 获取默认 QoS 级别
     *
     * @return 默认 QoS
     */
    MqttQoS getQos();

    /**
     * 发布消息（使用默认 QoS）
     *
     * @param topic   主题
     * @param payload 负载
     * @return 发布完成的 Mono
     */
    default Mono<Void> publish(String topic, ByteBuf payload) {
        return publish(topic, payload, getQos(), false);
    }

    /**
     * 发布消息
     *
     * @param topic   主题
     * @param payload 负载
     * @param qos     QoS 级别
     * @return 发布完成的 Mono（QoS > 0 时等待确认）
     */
    default Mono<Void> publish(String topic, ByteBuf payload, MqttQoS qos) {
        return publish(topic, payload, qos, false);
    }

    /**
     * 发布消息
     *
     * @param topic   主题
     * @param payload 负载
     * @param qos     QoS 级别
     * @param retain  是否保留
     * @return 发布完成的 Mono（QoS > 0 时等待确认）
     */
    Mono<Void> publish(String topic, ByteBuf payload, MqttQoS qos, boolean retain);

    default Disposable subscribe(String topic, Function<ClientReceivedPublish, Mono<Void>> handler) {
        return subscribe(topic, getQos(), handler);
    }

    default Disposable subscribe(Collection<String> topic, Function<ClientReceivedPublish, Mono<Void>> handler) {
        return subscribe(topic, getQos(), handler);
    }

    default Disposable subscribe(String topic, MqttQoS qos, Function<ClientReceivedPublish, Mono<Void>> handler) {
        return subscribe(List.of(topic), qos, handler);
    }

    /**
     * 订阅主题并处理消息（使用默认 QoS）
     *
     * <p>返回 Disposable，调用 dispose() 取消订阅。</p>
     *
     * @param topic   主题（支持通配符 + 和 #）
     * @param qos     qos
     * @param handler 消息处理器
     * @return Disposable 用于取消订阅
     */
    Disposable subscribe(Collection<String> topic, MqttQoS qos, Function<ClientReceivedPublish, Mono<Void>> handler);

    /**
     * 取消订阅
     *
     * @param topics 主题列表
     * @return 取消订阅完成的 Mono
     */
    Mono<Void> unsubscribe(String... topics);

    /**
     * 取消订阅
     *
     * @param topics 主题集合
     * @return 取消订阅完成的 Mono
     */
    Mono<Void> unsubscribe(Collection<String> topics);


    /**
     * 连接是否存活
     *
     * @return true 如果已连接
     */
    default boolean isConnected() {
        return isAlive();
    }

    /**
     * 优雅断开连接（发送 DISCONNECT 后关闭）
     *
     * @return 断开完成的 Mono
     */
    Mono<Void> disconnect();

    /**
     * 监听重连成功事件
     *
     * <p>每次重连成功时都会发射重连次数，可以持续监听多次重连事件。</p>
     *
     * <h3>使用示例：</h3>
     * <pre>{@code
     * Flux<Integer> reconnectEvents = connection.onReconnect()
     *     .doOnNext(attempt ->
     *         System.out.println("Reconnected after " + attempt + " attempts!"));
     * }</pre>
     *
     * @return 重连成功时发射重连次数的 Flux
     */
    Flux<Integer> onReconnect();
}
