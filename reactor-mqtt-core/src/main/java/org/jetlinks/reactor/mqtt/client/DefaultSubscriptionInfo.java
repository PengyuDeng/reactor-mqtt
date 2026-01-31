package org.jetlinks.reactor.mqtt.client;

import io.netty.handler.codec.mqtt.MqttQoS;

/**
 * 订阅信息默认实现
 *
 * @author PengyuDeng
 */
record DefaultSubscriptionInfo(String topic, MqttQoS qos) implements SubscriptionManager.SubscriptionInfo {
    static SubscriptionManager.SubscriptionInfo of(SubscriptionHandlers subscriptionHandlers) {
        return new DefaultSubscriptionInfo(subscriptionHandlers.getTopic(), subscriptionHandlers.getQos());
    }
}
