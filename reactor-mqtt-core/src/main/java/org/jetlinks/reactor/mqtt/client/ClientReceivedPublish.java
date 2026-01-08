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

import org.jetlinks.reactor.mqtt.ReceivedPublish;

/**
 * MQTT 客户端接收到的发布消息
 *
 * <p>封装从服务端接收到的 PUBLISH 消息，提供便捷的访问方法。</p>
 *
 * @author PengyuDeng
 */
public interface ClientReceivedPublish extends ReceivedPublish {
}
