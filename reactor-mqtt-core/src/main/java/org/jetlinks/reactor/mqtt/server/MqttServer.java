package org.jetlinks.reactor.mqtt.server;

import io.netty.handler.ssl.SslContext;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.resources.LoopResources;

import java.time.Duration;
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
        return new DefaultMqttServer();
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
     * 设置最大消息大小
     *
     * @param maxMessageSize 最大消息大小（字节）
     * @return 当前实例
     */
    MqttServer maxMessageSize(int maxMessageSize);

    /**
     * 设置空闲超时时间
     *
     * @param idleTimeout 空闲超时时间
     * @return 当前实例
     */
    MqttServer idleTimeout(Duration idleTimeout);

    /**
     * 设置 SSL 上下文
     *
     * @param sslContext SSL 上下文
     * @return 当前实例
     */
    MqttServer ssl(SslContext sslContext);

    /**
     * 设置事件循环资源
     *
     * @param loopResources 事件循环资源
     * @return 当前实例
     */
    MqttServer loopResources(LoopResources loopResources);

    /**
     * 设置工作线程数
     *
     * @param workerCount 工作线程数
     * @return 当前实例
     */
    MqttServer workerCount(int workerCount);

    /**
     * 设置 TCP_NODELAY 选项
     *
     * @param tcpNoDelay true 启用，false 禁用
     * @return 当前实例
     */
    MqttServer tcpNoDelay(boolean tcpNoDelay);

    /**
     * 设置 SO_KEEPALIVE 选项
     *
     * @param tcpKeepAlive true 启用，false 禁用
     * @return 当前实例
     */
    MqttServer tcpKeepAlive(boolean tcpKeepAlive);

    /**
     * 设置 SO_BACKLOG 选项
     *
     * @param soBacklog backlog 大小
     * @return 当前实例
     */
    MqttServer soBacklog(int soBacklog);

    /**
     * 设置写缓冲区水位标记
     *
     * @param low  低水位标记（字节）
     * @param high 高水位标记（字节）
     * @return 当前实例
     */
    MqttServer writeBufferWaterMark(int low, int high);

    /**
     * 设置连接处理器，在认证通过后调用
     *
     * <p>用于处理连接建立时的业务逻辑，如：</p>
     * <ul>
     *   <li>注册消息监听器（{@link ServerConnection#handlePublishing}、{@link ServerConnection#handleSubscribe} 等）</li>
     *   <li>检查客户端 ID 是否在黑名单</li>
     *   <li>限制最大连接数</li>
     *   <li>将连接注册到连接管理器</li>
     * </ul>
     *
     * <p>处理器必须调用 {@link ServerConnection#accept()} 或 {@link ServerConnection#reject} 来决定是否接受连接。</p>
     *
     * <pre>{@code
     * MqttServer.create()
     *     .authenticator(MqttAuthenticator.simple("user", "pass"))
     *     .handle(connection -> {
     *         connection.handlePublishing(msg -> {
     *             System.out.println("Received: " + msg.getTopic());
     *         });
     *         return connection.accept();
     *     })
     *     .bindNow();
     * }</pre>
     *
     * @param handler 连接处理器，接收 {@link ServerConnection}，返回 {@code Mono<Void>}
     * @return 当前实例
     */
    MqttServer handle(@Nullable Function<ServerConnection, Mono<Void>> handler);

    /**
     * 设置认证器
     *
     * @param authenticator 认证器实例
     * @return 当前实例
     */
    MqttServer authenticator(MqttAuthenticator authenticator);

    /**
     * 设置是否自动应答 QoS > 0 的消息
     * <p>
     * 当设置为 true（默认）时，服务端在处理完成后自动发送 ACK。
     * 当设置为 false 时，需要处理者手动调用 {@link ServerReceivedPublish#acknowledge()} 进行应答。
     * </p>
     *
     * @param autoAck true 自动应答（默认），false 手动应答
     * @return 当前实例
     */
    MqttServer autoAck(boolean autoAck);

    /**
     * 设置简单的用户名/密码认证
     *
     * @param username 用户名
     * @param password 密码
     * @return 当前实例
     */
    default MqttServer auth(String username, String password) {
        return authenticator(MqttAuthenticator.simple(username, password));
    }

    /**
     * 阻塞并绑定端口，启动服务
     *
     * @return DisposableServer 实例
     */
    DisposableServer bindNow();

    /**
     * 阻塞并绑定端口，启动服务（带超时）
     *
     * @param timeout 超时时间
     * @return DisposableServer 实例
     */
    DisposableServer bindNow(Duration timeout);

    /**
     * 响应式启动
     *
     * @return 包含 DisposableServer 的 Mono
     */
    Mono<? extends DisposableServer> bind();
}