/*
 * Copyright 2025 JetLinks https://www.jetlinks.cn
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

/**
 * MQTT 连接状态常量
 *
 * <p>使用位掩码管理连接状态，支持多状态组合。</p>
 *
 * <pre>
 * 状态位布局 (byte):
 * bit 0: CLOSED     - 连接已关闭
 * bit 1: ACCEPTED   - 连接已被接受
 * bit 2-7: 保留
 * </pre>
 *
 * @author PengyuDeng
 */
public interface MqttConnectionState {

    /**
     * 初始状态
     */
    byte STATE_INIT = 0;

    /**
     * 连接已关闭
     */
    byte STATE_CLOSED = 1;

    /**
     * 连接已被接受
     */
    byte STATE_ACCEPTED = 1 << 1;

    /**
     * 检查状态是否已关闭
     */
    static boolean isClosed(byte state) {
        return (state & STATE_CLOSED) != 0;
    }

    /**
     * 检查状态是否已接受
     */
    static boolean isAccepted(byte state) {
        return (state & STATE_ACCEPTED) != 0;
    }

    /**
     * 设置关闭标志
     */
    static byte setClosed(byte state) {
        return (byte) (state | STATE_CLOSED);
    }

    /**
     * 设置接受标志
     */
    static byte setAccepted(byte state) {
        return (byte) (state | STATE_ACCEPTED);
    }
}
