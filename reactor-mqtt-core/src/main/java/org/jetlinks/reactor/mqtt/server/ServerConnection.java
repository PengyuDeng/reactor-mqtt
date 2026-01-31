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
import org.jetlinks.reactor.mqtt.MqttAuth;
import org.jetlinks.reactor.mqtt.MqttConnection;
import org.jetlinks.reactor.mqtt.MqttWillMessage;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.function.Function;

/**
 * MQTT 服务端连接接口 - 纯响应式 API
 *
 * <p>扩展公共连接接口，增加服务端特有的方法。</p>
 *
 * <pre>{@code
 * MqttServer.create()
 *     .handle(connection -> {
 *         connection.handlePublishing(message -> {
 *             System.out.println("Received: " + message.topic());
 *             return Mono.empty();
 *         });
 *
 *         connection.handleSubscribe(subscription -> {
 *             System.out.println("Subscribe: " + subscription.getMessage());
 *             return Mono.empty();
 *         });
 *
 *         return connection.accept();
 *     })
 *     .bindNow();
 * }</pre>
 *
 * @author PengyuDeng
 */
public interface ServerConnection extends MqttConnection {

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
    MqttWillMessage getWill();

    /**
     * 处理客户端发布的消息
     *
     * <p>当客户端发布消息到服务器时调用此方法。
     * 对于 QoS 1/2 消息，ACK 会在返回的 Mono 完成后自动发送。</p>
     *
     * @param handler 消息处理器，返回处理完成的 Mono
     * @return this
     */
    ServerConnection handlePublishing(Function<ServerReceivedPublish, Mono<Void>> handler);

    /**
     * 处理客户端的订阅请求
     *
     * <p>当客户端发送订阅请求时调用此方法。
     * SUBACK 会在返回的 Mono 完成后自动发送。</p>
     *
     * @param handler 订阅处理器，返回处理完成的 Mono
     * @return this
     */
    ServerConnection handleSubscribe(Function<MqttSubscription, Mono<Void>> handler);

    /**
     * 处理客户端的取消订阅请求
     *
     * <p>当客户端发送取消订阅请求时调用此方法。
     * UNSUBACK 会在返回的 Mono 完成后自动发送。</p>
     *
     * @param handler 取消订阅处理器，返回处理完成的 Mono
     * @return this
     */
    ServerConnection handleUnsubscribe(Function<MqttUnsubscription, Mono<Void>> handler);


    /**
     * 设置是否自动应答 QoS > 0 的消息
     * <p>
     * 当设置为 true（默认）时，服务端在处理完成后自动发送 ACK。
     * 当设置为 false 时，需要处理者手动调用 {@link ServerReceivedPublish#ack()} 进行应答。
     * </p>
     *
     * <p>手动应答示例：</p>
     * <pre>{@code
     * connection.autoAck(false)
     *           .handlePublishing(message -> {
     *               return saveToDatabase(message)
     *                   .then(message.ack());  // 持久化成功后再应答
     *           });
     * }</pre>
     *
     * @param autoAck true 自动应答（默认），false 手动应答
     * @return 当前连接实例（支持链式调用）
     */
    ServerConnection autoAck(boolean autoAck);

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
}
