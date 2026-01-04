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
package org.jetlinks.reactor.mqtt;

/**
 * MQTT 主题匹配工具类（零分配实现）
 *
 * <p>支持 MQTT 通配符:</p>
 * <ul>
 *   <li>{@code +} - 单层通配符，匹配一个层级</li>
 *   <li>{@code #} - 多层通配符，匹配零个或多个层级（必须在末尾）</li>
 * </ul>
 *
 * <p>示例:</p>
 * <ul>
 *   <li>{@code sensor/+/temp} 匹配 {@code sensor/room1/temp}</li>
 *   <li>{@code sensor/#} 匹配 {@code sensor/room1/temp} 和 {@code sensor}</li>
 * </ul>
 *
 * @author PengyuDeng
 */
public final class TopicMatcher {

    private TopicMatcher() {
    }

    /**
     * 检查主题是否匹配过滤器
     *
     * @param filter 订阅过滤器（可含通配符）
     * @param topic  实际主题
     * @return true 如果匹配
     */
    public static boolean matches(String filter, String topic) {
        if (filter.equals(topic)) {
            return true;
        }

        int filterLen = filter.length();
        int topicLen = topic.length();
        int filterIdx = 0;
        int topicIdx = 0;

        while (filterIdx < filterLen && topicIdx < topicLen) {
            char fc = filter.charAt(filterIdx);

            // 多层通配符 #，匹配剩余所有
            if (fc == '#') {
                return true;
            }

            // 单层通配符 +，跳过 topic 当前层
            if (fc == '+') {
                filterIdx++;
                while (topicIdx < topicLen && topic.charAt(topicIdx) != '/') {
                    topicIdx++;
                }
            } else {
                // 精确匹配
                if (fc != topic.charAt(topicIdx)) {
                    return false;
                }
                filterIdx++;
                topicIdx++;
            }
        }

        // 都消费完
        if (filterIdx == filterLen && topicIdx == topicLen) {
            return true;
        }

        // filter 剩余 # 可以匹配空
        if (filterIdx < filterLen && filter.charAt(filterIdx) == '#') {
            return true;
        }

        // filter 剩余 /# 可以匹配空（如 a/b/# 匹配 a/b）
        if (filterIdx + 1 < filterLen
                && filter.charAt(filterIdx) == '/'
                && filter.charAt(filterIdx + 1) == '#') {
            return topicIdx == topicLen;
        }

        return false;
    }
}
