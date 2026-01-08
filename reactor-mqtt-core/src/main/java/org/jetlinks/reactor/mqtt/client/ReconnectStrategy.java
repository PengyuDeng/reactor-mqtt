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
package org.jetlinks.reactor.mqtt.client;

import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * MQTT 客户端重连策略
 *
 * <p>定义连接断开后的重连行为，支持多种内置策略。</p>
 *
 * <h3>使用示例：</h3>
 * <pre>{@code
 * // 固定延迟重连
 * MqttClient.create()
 *     .reconnectStrategy(ReconnectStrategy.fixedDelay(Duration.ofSeconds(5)))
 *     .connectNow();
 *
 * // 指数退避重连
 * MqttClient.create()
 *     .reconnectStrategy(ReconnectStrategy.exponentialBackoff(
 *         Duration.ofSeconds(1),
 *         Duration.ofMinutes(5)))
 *     .connectNow();
 *
 * // 禁用重连
 * MqttClient.create()
 *     .reconnectStrategy(ReconnectStrategy.none())
 *     .connectNow();
 * }</pre>
 *
 * @author PengyuDeng
 */
public interface ReconnectStrategy {

    /**
     * 计算下次重连的延迟时间
     *
     * @param attempt   重连尝试次数（从 1 开始）
     * @param lastError 上次连接失败的异常
     * @return 延迟时间的 Mono，返回空 Mono 表示停止重连
     */
    Mono<Duration> nextDelay(int attempt, Throwable lastError);

    /**
     * 固定延迟重连策略
     *
     * @param delay 每次重连的固定延迟
     * @return 重连策略
     */
    static ReconnectStrategy fixedDelay(Duration delay) {
        return (attempt, lastError) -> Mono.just(delay);
    }

    /**
     * 指数退避重连策略
     *
     * <p>延迟时间 = min(initial * 2^(attempt-1), max)</p>
     *
     * @param initial 初始延迟
     * @param max     最大延迟
     * @return 重连策略
     */
    static ReconnectStrategy exponentialBackoff(Duration initial, Duration max) {
        return (attempt, lastError) -> {
            long delayMs = initial.toMillis() * (1L << Math.min(attempt - 1, 30));
            delayMs = Math.min(delayMs, max.toMillis());
            return Mono.just(Duration.ofMillis(delayMs));
        };
    }

    /**
     * 带最大重试次数的指数退避策略
     *
     * @param initial    初始延迟
     * @param max        最大延迟
     * @param maxRetries 最大重试次数，超过后停止重连
     * @return 重连策略
     */
    static ReconnectStrategy exponentialBackoff(Duration initial, Duration max, int maxRetries) {
        return (attempt, lastError) -> {
            if (attempt > maxRetries) {
                return Mono.empty();
            }
            long delayMs = initial.toMillis() * (1L << Math.min(attempt - 1, 30));
            delayMs = Math.min(delayMs, max.toMillis());
            return Mono.just(Duration.ofMillis(delayMs));
        };
    }

    /**
     * 禁用重连
     *
     * @return 不重连的策略
     */
    static ReconnectStrategy none() {
        return (attempt, lastError) -> Mono.empty();
    }
}
