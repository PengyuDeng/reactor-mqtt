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

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.jetlinks.reactor.mqtt.server.messages.MqttPublishMessage;
import org.jetlinks.reactor.mqtt.server.messages.MqttSubscribeMessage;
import org.jetlinks.reactor.mqtt.server.messages.MqttUnsubscribeMessage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.SocketAddress;
import java.time.Duration;
import java.util.function.Function;

/**
 * MQTT 端点接口
 * <p>
 * 代表与远程 MQTT 客户端的点对点通信端点。
 * 提供响应式 API 处理 MQTT 消息。
 * </p>
 *
 * @author PengyuDeng
 */
public interface MqttEndpoint {

    /**
     * 获取客户端标识符
     *
     * @return 客户端 ID
     */
    String clientIdentifier();

    /**
     * 获取认证信息
     *
     * @return 认证信息（用户名/密码）
     */
    MqttAuth auth();

    /**
     * 获取遗言信息
     *
     * @return 遗言消息
     */
    MqttWill will();

    /**
     * 获取协议版本
     *
     * @return MQTT 协议版本（3=3.1, 4=3.1.1, 5=5.0）
     */
    int protocolVersion();

    /**
     * 是否为 Clean Session
     *
     * @return true 如果请求清除会话
     */
    boolean isCleanSession();

    /**
     * 获取 Keep Alive 超时时间（秒）
     *
     * @return keep alive 秒数
     */
    int keepAliveTimeSeconds();

    /**
     * 获取 CONNECT 消息的 MQTT 5.0 属性
     *
     * @return MQTT 属性
     */
    MqttProperties connectProperties();

    /**
     * 连接是否已建立
     *
     * @return true 如果已接受连接
     */
    boolean isConnected();

    /**
     * 端点是否已关闭
     *
     * @return true 如果已关闭
     */
    boolean isClosed();

    /**
     * 获取远程地址
     *
     * @return 客户端地址
     */
    SocketAddress remoteAddress();

    /**
     * 获取本地地址
     *
     * @return 服务器地址
     */
    SocketAddress localAddress();

    /**
     * 是否为 SSL 连接
     *
     * @return true 如果是 SSL/TLS 连接
     */
    boolean isSsl();

    /**
     * 接受连接请求
     *
     * @return 完成的 Mono
     */
    Mono<Void> accept();

    /**
     * 接受连接请求
     *
     * @param sessionPresent 是否存在先前会话
     * @return 完成的 Mono
     */
    Mono<Void> accept(boolean sessionPresent);

    /**
     * 拒绝连接请求
     *
     * @param returnCode 拒绝原因代码
     * @return 完成的 Mono
     */
    Mono<Void> reject(MqttConnectReturnCode returnCode);

    /**
     * 关闭端点
     *
     * @return 完成的 Mono
     */
    Mono<Void> close();

    /**
     * 设置发布消息自动 ACK
     *
     * @param autoAck 是否自动发送 PUBACK/PUBREC
     * @return this
     */
    MqttEndpoint publishAutoAck(boolean autoAck);

    /**
     * 获取发布消息自动 ACK 状态
     *
     * @return true 如果自动 ACK 开启
     */
    boolean isPublishAutoAck();

    /**
     * 设置订阅请求自动 ACK
     *
     * @param autoAck 是否自动发送 SUBACK/UNSUBACK
     * @return this
     */
    MqttEndpoint subscriptionAutoAck(boolean autoAck);

    /**
     * 获取订阅请求自动 ACK 状态
     *
     * @return true 如果自动 ACK 开启
     */
    boolean isSubscriptionAutoAck();

    /**
     * 设置 PUBLISH 消息处理器
     *
     * @param handler 消息处理函数
     * @return this
     */
    MqttEndpoint publishHandler(Function<MqttPublishMessage, Mono<Void>> handler);

    /**
     * 设置 SUBSCRIBE 消息处理器
     *
     * @param handler 订阅处理函数
     * @return this
     */
    MqttEndpoint subscribeHandler(Function<MqttSubscribeMessage, Mono<Void>> handler);

    /**
     * 设置 UNSUBSCRIBE 消息处理器
     *
     * @param handler 取消订阅处理函数
     * @return this
     */
    MqttEndpoint unsubscribeHandler(Function<MqttUnsubscribeMessage, Mono<Void>> handler);

    /**
     * 设置 PINGREQ 消息处理器
     *
     * @param handler ping 处理函数
     * @return this
     */
    MqttEndpoint pingHandler(Runnable handler);

    /**
     * 设置断开连接处理器
     *
     * @param handler 断开处理函数
     * @return this
     */
    MqttEndpoint disconnectHandler(Runnable handler);

    /**
     * 设置关闭处理器
     *
     * @param handler 关闭处理函数
     * @return this
     */
    MqttEndpoint closeHandler(Runnable handler);

    /**
     * 设置异常处理器
     *
     * @param handler 异常处理函数
     * @return this
     */
    MqttEndpoint exceptionHandler(java.util.function.Consumer<Throwable> handler);

    /**
     * 获取 PUBLISH 消息流
     *
     * @return 消息流
     */
    Flux<MqttPublishMessage> receivePublish();

    /**
     * 获取 SUBSCRIBE 消息流
     *
     * @return 订阅请求流
     */
    Flux<MqttSubscribeMessage> receiveSubscribe();

    /**
     * 获取 UNSUBSCRIBE 消息流
     *
     * @return 取消订阅请求流
     */
    Flux<MqttUnsubscribeMessage> receiveUnsubscribe();

    /**
     * 向客户端发布消息
     *
     * @param topic    主题
     * @param payload  负载
     * @param qosLevel QoS 级别
     * @param isDup    是否重复
     * @param isRetain 是否保留
     * @return 消息 ID
     */
    Mono<Integer> publish(String topic, ByteBuf payload, MqttQoS qosLevel, boolean isDup, boolean isRetain);

    /**
     * 向客户端发布消息（简化版）
     *
     * @param topic   主题
     * @param payload 负载
     * @param qos     QoS 级别
     * @return 消息 ID
     */
    default Mono<Integer> publish(String topic, ByteBuf payload, MqttQoS qos) {
        return publish(topic, payload, qos, false, false);
    }

    /**
     * 向客户端发布 QoS 0 消息
     *
     * @param topic   主题
     * @param payload 负载
     * @return 完成的 Mono
     */
    default Mono<Void> publish(String topic, ByteBuf payload) {
        return publish(topic, payload, MqttQoS.AT_MOST_ONCE, false, false).then();
    }

    /**
     * 端点关闭事件
     *
     * @return 关闭时完成的 Mono
     */
    Mono<Void> onClose();

    /**
     * 获取最后一次 ping 时间
     *
     * @return 最后 ping 时间戳
     */
    long lastPingTime();

    /**
     * 获取 keep alive 超时时长
     *
     * @return 超时时长
     */
    Duration keepAliveTimeout();
}
