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

import io.netty.handler.codec.mqtt.MqttQoS;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 默认订阅管理器实现
 *
 * @author PengyuDeng
 */
public class DefaultSubscriptionManager implements SubscriptionManager {

    private final Map<String, SubscriptionHandler> subscriptions = new ConcurrentHashMap<>();

    @Override
    public Disposable subscribe(MqttClientConnection connection,
                                CharSequence topic,
                                MqttQoS qos,
                                Function<MqttClientPublishing, Mono<Void>> handler) {
        String topicStr = topic.toString();
        SubscriptionHandler subHandler = new SubscriptionHandler(topicStr, qos, handler);
        subscriptions.put(topicStr, subHandler);

        Disposable.Composite composite = Disposables.composite();

        // 发送订阅请求
        composite.add(connection.subscribe(topicStr).subscribe());

        // 取消时移除订阅
        composite.add(() -> {
            subscriptions.remove(topicStr);
            connection.unsubscribe(topicStr).subscribe();
        });

        return composite;
    }

    @Override
    public Mono<Void> handleMessage(MqttClientPublishing publishing) {
        String topic = publishing.getTopic();
        Mono<Void> result = Mono.empty();

        for (Map.Entry<String, SubscriptionHandler> entry : subscriptions.entrySet()) {
            if (topicMatches(entry.getKey(), topic)) {
                SubscriptionHandler handler = entry.getValue();
                if (handler.handler != null) {
                    result = result.then(handler.handler.apply(publishing));
                }
            }
        }

        return result;
    }

    @Override
    public Iterable<SubscriptionInfo> getSubscriptions() {
        return subscriptions.values()
                           .stream()
                           .map(h -> (SubscriptionInfo) new SubscriptionInfoImpl(h.topic, h.qos))
                           .collect(Collectors.toList());
    }

    @Override
    public void clear() {
        subscriptions.clear();
    }

    /**
     * MQTT 主题匹配
     */
    private boolean topicMatches(String filter, String topic) {
        if (filter.equals(topic)) {
            return true;
        }

        String[] filterParts = filter.split("/");
        String[] topicParts = topic.split("/");

        for (int i = 0; i < filterParts.length; i++) {
            String filterPart = filterParts[i];

            if (filterPart.equals("#")) {
                return true;
            }

            if (i >= topicParts.length) {
                return false;
            }

            if (!filterPart.equals("+") && !filterPart.equals(topicParts[i])) {
                return false;
            }
        }

        return filterParts.length == topicParts.length;
    }

    private static class SubscriptionHandler {
        final String topic;
        final MqttQoS qos;
        final Function<MqttClientPublishing, Mono<Void>> handler;

        SubscriptionHandler(String topic, MqttQoS qos, Function<MqttClientPublishing, Mono<Void>> handler) {
            this.topic = topic;
            this.qos = qos;
            this.handler = handler;
        }
    }

    private record SubscriptionInfoImpl(String topic, MqttQoS qos) implements SubscriptionInfo {
    }
}
