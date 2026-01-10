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
 * 简单的用户名/密码认证器
 *
 * @author PengyuDeng
 */
class SimpleMqttAuthenticator implements MqttAuthenticator {

    private static final Logger log = Logger.getLogger(SimpleMqttAuthenticator.class.getName());

    private final String username;
    private final String password;

    SimpleMqttAuthenticator(String username, String password) {
        this.username = username;
        this.password = password;
    }

    @Override
    public Mono<Boolean> authenticate(ServerConnection connection) {
        return Mono.fromSupplier(() -> {
            MqttAuth auth = connection.getAuth();

            // 检查是否提供了认证信息
            if (auth == null || !auth.hasAuth()) {
                log.log(Level.WARNING, "Client " + connection.getClientId() + " authentication failed: no credentials provided");
                return false;
            }

            // 验证用户名和密码
            boolean isValid = Objects.equals(username, auth.getUsername())
                           && Objects.equals(password, auth.getPassword());

            if (!isValid) {
                log.log(Level.WARNING, "Client " + connection.getClientId() + " authentication failed: invalid credentials");
            } else {
                log.log(Level.FINE, "Client " + connection.getClientId() + " authenticated successfully");
            }

            return isValid;
        });
    }
}
