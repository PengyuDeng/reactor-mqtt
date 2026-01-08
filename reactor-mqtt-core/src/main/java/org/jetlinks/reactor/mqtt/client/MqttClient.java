package org.jetlinks.reactor.mqtt.client;

import reactor.core.publisher.Mono;

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
    default MqttClient create() {
        //TODO
        return null;
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
     * 阻塞并连接到服务器
     *
     * @return 连接实例
     */
    ClientConnection connectNow();

    /**
     * 异步连接到服务器
     *
     * @return 连接实例
     */
    Mono<ClientConnection> connect();
}