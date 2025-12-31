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

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;

/**
 * MQTT 遗言（Last Will）消息
 * <p>
 * 当客户端异常断开时，Broker 会发布此消息到指定主题。
 * </p>
 *
 * @author PengyuDeng
 */
public class MqttWill {

    private final boolean willFlag;

    private final String willTopic;

    private final ByteBuf willMessage;

    private final MqttQoS willQos;

    private final boolean willRetain;

    private final MqttProperties willProperties;
    /**
     * 空遗言
     */
    public final static MqttWill EMPTY = new MqttWill(false, null, null, null, false, null);


    /**
     * 构造函数
     *
     * @param willFlag       是否有遗言
     * @param willTopic      遗言主题
     * @param willMessage    遗言内容
     * @param willQos        遗言 QoS
     * @param willRetain     是否保留
     * @param willProperties MQTT 5.0 属性
     */
    public MqttWill(boolean willFlag, String willTopic, ByteBuf willMessage,
                    MqttQoS willQos, boolean willRetain, MqttProperties willProperties) {
        this.willFlag = willFlag;
        this.willTopic = willTopic;
        this.willMessage = willMessage;
        this.willQos = willQos;
        this.willRetain = willRetain;
        this.willProperties = willProperties != null ? willProperties : MqttProperties.NO_PROPERTIES;
    }

    /**
     * 是否有遗言
     */
    public boolean isWillFlag() {
        return willFlag;
    }

    /**
     * 获取遗言主题
     */
    public String getWillTopic() {
        return willTopic;
    }

    /**
     * 获取遗言消息内容
     */
    public ByteBuf getWillMessage() {
        return willMessage;
    }

    /**
     * 获取遗言 QoS 级别
     */
    public MqttQoS getWillQos() {
        return willQos;
    }

    /**
     * 是否保留遗言
     */
    public boolean isWillRetain() {
        return willRetain;
    }

    /**
     * 获取遗言 MQTT 5.0 属性
     */
    public MqttProperties getWillProperties() {
        return willProperties;
    }

    @Override
    public String toString() {
        if (!willFlag) {
            return "MqttWill{none}";
        }
        return "MqttWill{" +
                "topic='" + willTopic + '\'' +
                ", qos=" + willQos +
                ", retain=" + willRetain +
                '}';
    }
}
