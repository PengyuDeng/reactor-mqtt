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

import org.jspecify.annotations.NonNull;
import reactor.netty.DisposableServer;

import java.time.Duration;
import java.util.logging.Logger;

/**
 * MQTT Broker 使用示例
 *
 * @author PengyuDeng
 */
public class MqttBrokerExample {

    private static final Logger log = Logger.getLogger(MqttBrokerExample.class.getName());

    public static void main(String[] args) throws InterruptedException {
        // 创建并启动 MQTT Broker
        DisposableServer broker = MqttBroker.create()
                                     .host("0.0.0.0")
                                     .port(1883)
                                     .idleTimeout(Duration.ofSeconds(120))
                                     .bindNow();

        log.info("MQTT Broker started on port 1883");
        log.info("Waiting for connections...");

        // 定期打印统计信息
        Thread statsThread = getThread((MqttBroker) broker);

        // 等待服务器关闭
        broker.onDispose().block();
        statsThread.interrupt();
    }

    private static @NonNull Thread getThread(MqttBroker broker) {
        Thread statsThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(5000);
                    log.info("Broker Statistics - Connections: " + broker.getConnectionCount() +
                            ", Subscriptions: " + broker.getSubscriptionCount());
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        statsThread.start();
        return statsThread;
    }

    /**
     * 使用自定义配置的示例
     */
    public static void withCustomConfiguration() {
        DisposableServer broker = MqttBroker.create()
                                     .host("0.0.0.0")
                                     .port(1883)
                                     .maxMessageSize(10240)
                                     .idleTimeout(Duration.ofSeconds(60))
                                     .workerCount(4)
                                     .tcpNoDelay(true)
                                     .tcpKeepAlive(true)
                                     .bindNow();

        log.info("MQTT Broker with custom configuration started");

        // 可以在这里添加自定义的业务逻辑
        // 例如: 监控、日志记录、持久化等

        broker.onDispose().block();
    }
}
