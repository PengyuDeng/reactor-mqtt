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
package org.jetlinks.reactor.mqtt.broker;

import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import reactor.netty.DisposableServer;

import java.time.Duration;
import java.util.logging.Logger;

/**
 * MQTT Broker 认证示例
 *
 * @author PengyuDeng
 */
public class MqttBrokerAuthExample {

    private static final Logger log = Logger.getLogger(MqttBrokerAuthExample.class.getName());

    public static void main(String[] args) throws InterruptedException {
        // 创建带认证的 MQTT Broker
        DisposableServer broker = MqttBroker.create()
                                            .host("0.0.0.0")
                                            .port(1883)
                                            .auth("admin", "password123")  // 设置用户名和密码
                                            .idleTimeout(Duration.ofSeconds(120))
                                            .bindNow();

        log.info("MQTT Broker started on port 1883 with authentication enabled");
        log.info("Username: admin, Password: password123");
        log.info("Waiting for connections...");

        // 等待服务器关闭
        broker.onDispose().block();
    }

    /**
     * 使用自定义认证器的示例
     */
    public static void withCustomAuthenticator() {
        DisposableServer broker = MqttBroker.create()
                                            .host("0.0.0.0")
                                            .port(1883)
                                            // 使用自定义认证器
                                            .authenticator(connection -> {
                                                String clientId = connection.getClientId();
                                                String username = connection.getAuth().getUsername();

                                                // 自定义认证逻辑
                                                // 例如：从数据库验证，或者根据 clientId 进行特殊处理
                                                log.info("Authenticating client: " + clientId + ", username: " + username);

                                                // 这里可以实现更复杂的认证逻辑
                                                return reactor.core.publisher.Mono.just(MqttConnectReturnCode.CONNECTION_ACCEPTED);
                                            })
                                            .bindNow();

        log.info("MQTT Broker with custom authenticator started");
        broker.onDispose().block();
    }

    /**
     * 允许匿名连接的示例
     */
    public static void allowAnonymous() {
        DisposableServer broker = MqttBroker.create()
                                            .host("0.0.0.0")
                                            .port(1883)
                                            // 不设置认证器，默认允许匿名连接
                                            .bindNow();

        log.info("MQTT Broker started without authentication (anonymous allowed)");
        broker.onDispose().block();
    }
}
