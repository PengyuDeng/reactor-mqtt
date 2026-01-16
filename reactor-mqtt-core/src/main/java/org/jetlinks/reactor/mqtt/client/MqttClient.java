package org.jetlinks.reactor.mqtt.client;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.handler.ssl.SslContext;
import org.jetlinks.reactor.mqtt.MqttWillMessage;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;

import java.time.Duration;
import java.util.function.Function;

/**
 * MQTT 客户端接口
 *
 * <p>提供流式 API 配置并创建 {@link ClientConnection MQTT 连接}。</p>
 *
 * @author PengyuDeng
 * @since 1.0.0
 */
public interface MqttClient {

    /**
     * 创建一个新的客户端构建器
     *
     * @return 客户端构建器
     */
    static MqttClient create() {
        return new DefaultMqttClient();
    }

    /**
     * 设置服务器地址
     *
     * @param host 服务器地址
     * @return 当前实例
     */
    MqttClient host(String host);

    /**
     * 设置服务器端口
     *
     * @param port 服务器端口
     * @return 当前实例
     */
    MqttClient port(int port);

    /**
     * 设置客户端标识符
     *
     * @param clientId 客户端标识符
     * @return 当前实例
     */
    MqttClient clientId(String clientId);

    /**
     * 设置消息处理器
     *
     * @param handler 处理器
     * @return 当前实例
     */
    MqttClient handlePublishing(Function<ClientReceivedPublish, Mono<Void>> handler);

    /**
     * 设置认证信息
     *
     * @param username 用户名
     * @param password 密码
     * @return 当前实例
     */
    MqttClient auth(String username, String password);

    /**
     * 设置认证信息
     *
     * @param username 用户名
     * @param password 密码（字节数组）
     * @return 当前实例
     */
    MqttClient auth(String username, byte[] password);

    /**
     * 设置心跳间隔
     *
     * @param seconds 心跳间隔（秒）
     * @return 当前实例
     */
    MqttClient keepAlive(int seconds);

    /**
     * 设置是否清除会话
     *
     * @param cleanSession true 清除会话，false 保持会话
     * @return 当前实例
     */
    MqttClient cleanSession(boolean cleanSession);

    /**
     * 设置 MQTT 协议版本
     *
     * @param version MQTT 协议版本
     * @return 当前实例
     */
    MqttClient protocolVersion(MqttVersion version);

    /**
     * 设置最大消息大小
     *
     * @param maxMessageSize 最大消息大小（字节）
     * @return 当前实例
     */
    MqttClient maxMessageSize(int maxMessageSize);

    /**
     * 设置遗言消息
     *
     * @param topic   遗言主题
     * @param payload 遗言消息内容
     * @param qos     QoS 级别
     * @param retain  是否保留
     * @return 当前实例
     */
    MqttClient will(String topic, ByteBuf payload, MqttQoS qos, boolean retain);

    /**
     * 设置遗言消息
     *
     * @param topic   遗言主题
     * @param payload 遗言消息内容
     * @param qos     QoS 级别
     * @param retain  是否保留
     * @return 当前实例
     */
    MqttClient will(String topic, byte[] payload, MqttQoS qos, boolean retain);

    /**
     * 设置遗言消息
     *
     * @param willMessage 遗言消息对象
     * @return 当前实例
     */
    MqttClient will(MqttWillMessage willMessage);

    /**
     * 设置 SSL 上下文
     *
     * @param sslContext SSL 上下文
     * @return 当前实例
     */
    MqttClient ssl(SslContext sslContext);

    /**
     * 设置重连策略
     *
     * @param strategy 重连策略
     * @return 当前实例
     */
    MqttClient reconnectStrategy(ReconnectStrategy strategy);

    /**
     * 设置是否自动重新订阅
     *
     * @param autoResubscribe true 自动重新订阅，false 不自动重新订阅
     * @return 当前实例
     */
    MqttClient autoResubscribe(boolean autoResubscribe);

    /**
     * 设置是否自动确认消息
     *
     * @param autoAck true 自动确认，false 手动确认
     * @return 当前实例
     */
    MqttClient autoAck(boolean autoAck);

    /**
     * 设置默认 QoS 级别
     *
     * @param qos QoS 级别
     * @return 当前实例
     */
    MqttClient qos(MqttQoS qos);

    /**
     * 设置事件循环资源
     *
     * @param loopResources 事件循环资源
     * @return 当前实例
     */
    MqttClient loopResources(LoopResources loopResources);

    /**
     * 设置 TCP_NODELAY 选项
     *
     * @param tcpNoDelay true 启用，false 禁用
     * @return 当前实例
     */
    MqttClient tcpNoDelay(boolean tcpNoDelay);

    /**
     * 设置连接超时时间
     *
     * @param timeout 超时时间
     * @return 当前实例
     */
    MqttClient connectTimeout(Duration timeout);

    /**
     * 设置订阅超时时间
     *
     * @param timeout 超时时间
     * @return 当前实例
     */
    MqttClient subscribeTimeout(Duration timeout);

    /**
     * 设置取消订阅超时时间
     *
     * @param timeout 超时时间
     * @return 当前实例
     */
    MqttClient unsubscribeTimeout(Duration timeout);

    /**
     * 设置发布超时时间
     *
     * @param timeout 超时时间
     * @return 当前实例
     */
    MqttClient publishTimeout(Duration timeout);

    /**
     * 设置订阅管理器
     *
     * <p>允许用户自定义订阅管理器实现，或选择不同的内置实现：</p>
     * <ul>
     *     <li>{@link SubscriptionManager#create()} - 默认实现，适合少量订阅</li>
     *     <li>{@link SubscriptionManager#createTrieBased()} - 基于Trie树的高性能实现，适合大量订阅</li>
     * </ul>
     *
     * @param subscriptionManager 订阅管理器实例
     * @return 当前实例
     */
    MqttClient subscriptionManager(SubscriptionManager subscriptionManager);

    /**
     * 阻塞并连接到服务器
     *
     * @return 连接实例
     */
    ClientConnection connectNow();

    /**
     * 异步连接到服务器
     *
     * @return 连接实例的 Mono
     */
    Mono<ClientConnection> connect();
}