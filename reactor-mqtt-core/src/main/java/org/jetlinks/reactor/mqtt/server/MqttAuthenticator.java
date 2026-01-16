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

import org.jetlinks.reactor.mqtt.MqttAuth;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MQTT 认证器接口
 *
 * <p>用于验证客户端连接的用户名和密码</p>
 *
 * @author PengyuDeng
 */
public interface MqttAuthenticator {

    Logger log = Logger.getLogger(MqttAuthenticator.class.getName());

    /**
     * 验证客户端连接
     *
     * @param connection 服务端连接
     * @return 认证结果，true 表示通过，false 表示拒绝
     */
    Mono<Boolean> authenticate(ServerConnection connection);

    /**
     * 创建一个简单的用户名/密码认证器
     *
     * @param username 用户名
     * @param password 密码
     * @return 认证器实例
     */
    static MqttAuthenticator simple(String username, String password) {
        return connection -> Mono.fromSupplier(() -> {
            MqttAuth auth = connection.getAuth();

            // 检查是否提供了认证信息
            if (auth == null || !auth.hasAuth()) {
                log.log(Level.WARNING, () -> "Client " + connection.getClientId() + " authentication failed: no credentials provided");
                return false;
            }

            // 验证用户名和密码
            boolean isValid = Objects.equals(username, auth.getUsername())
                           && Objects.equals(password, auth.getPassword());

            if (!isValid) {
                log.log(Level.WARNING, () -> "Client " + connection.getClientId() + " authentication failed: invalid credentials");
            } else {
                log.log(Level.FINE, () -> "Client " + connection.getClientId() + " authenticated successfully");
            }

            return isValid;
        });
    }

    /**
     * 创建一个允许匿名连接的认证器（无认证）
     *
     * @return 认证器实例
     */
    static MqttAuthenticator allowAnonymous() {
        return connection -> Mono.just(true);
    }

    /**
     * 创建一个拒绝所有连接的认证器
     *
     * @return 认证器实例
     */
    static MqttAuthenticator denyAll() {
        return connection -> Mono.just(false);
    }
}
