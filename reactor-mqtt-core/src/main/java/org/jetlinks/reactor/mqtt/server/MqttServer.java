package org.jetlinks.reactor.mqtt.server;

import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;

import java.util.function.Function;

/**
 * MQTT 服务端接口
 *
 * <p>提供流式 API 配置并启动 {@link DisposableServer MQTT 服务}。</p>
 *
 * @author PengyuDeng
 * @since 1.0.0
 */
public interface MqttServer {

    /**
     * 创建一个新的服务器构建器
     *
     * @return 服务器构建器
     */
    static MqttServer create() {
        // TODO
        return null;
    }

    /**
     * 设置绑定地址
     *
     * @param host 绑定地址
     * @return 当前实例
     */
    MqttServer host(String host);

    /**
     * 设置绑定端口
     *
     * @param port 绑定端口
     * @return 当前实例
     */
    MqttServer port(int port);

    /**
     * 核心处理器：定义每一个连接建立时的行为
     *
     * @param handler 接收 {@link ServerConnection}，返回 Mono<Void>
     *                (通常是 validate.then(accept))
     * @return 当前实例
     */
    MqttServer handle(Function<ServerConnection, Mono<Void>> handler);

    /**
     * 阻塞并绑定端口，启动服务
     *
     * @return DisposableServer 实例
     */
    DisposableServer bindNow();

    /**
     * 响应式启动
     *
     * @return 包含 DisposableServer 的 Mono
     */
    Mono<? extends DisposableServer> bind();
}