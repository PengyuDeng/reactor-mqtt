package org.jetlinks.reactor.mqtt;

import reactor.core.publisher.Mono;

public interface Acknowledge {
    /**
     * 确认消息（发送 ACK）
     * <p>
     * QoS 0: 无操作<br>
     * QoS 1: 发送 PUBACK<br>
     * QoS 2: 发送 PUBREC
     * </p>
     *
     * @return 确认完成的 Mono
     */
    Mono<Void> acknowledge();
}
