package org.jetlinks.reactor.mqtt.client;

import io.netty.handler.codec.mqtt.MqttQoS;

record DefaultSubscriptionInfo(String topic, MqttQoS qos) implements SubscriptionManager.SubscriptionInfo {
    static SubscriptionManager.SubscriptionInfo of(SubscriptionHandlers subscriptionHandlers) {
        return new DefaultSubscriptionInfo(subscriptionHandlers.getTopic(), subscriptionHandlers.getQos());
    }
}
