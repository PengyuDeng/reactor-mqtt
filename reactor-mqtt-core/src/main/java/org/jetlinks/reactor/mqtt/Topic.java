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
 * MQTT 主题接口
 *
 * <p>封装主题相关的操作，避免重复的字符串解析和匹配操作。</p>
 *
 * @author PengyuDeng
 * @see ParsedTopic
 */
public interface Topic {

    /**
     * 解析主题字符串，返回 Topic 实例
     *
     * <p>内部使用缓存，对于相同的主题字符串会返回相同的实例。</p>
     *
     * @param topic 主题字符串
     * @return Topic 实例
     */
    static Topic of(String topic) {
        return ParsedTopic.parse(topic);
    }

    /**
     * 获取原始主题字符串
     *
     * @return 主题字符串
     */
    String getValue();

    /**
     * 获取预解析的主题层级数组
     * 获取主题层级数组（零拷贝，只读，请勿修改！）
     *
     * <p><b>警告</b>：此方法返回内部数组引用以提升性能，
     * 调用方必须承诺不修改返回的数组。
     * 如果需要修改，请自行拷贝：{@code Arrays.copyOf(levels, levels.length)}</p>
     */
    String[] getLevels();

    /**
     * 检查当前主题是否匹配指定的过滤器
     *
     * <p>支持 MQTT 通配符:</p>
     * <ul>
     *   <li>{@code +} - 单层通配符，匹配一个层级</li>
     *   <li>{@code #} - 多层通配符，匹配零个或多个层级（必须在末尾）</li>
     * </ul>
     *
     * @param filter 订阅过滤器（可含通配符）
     * @return true 如果匹配
     */
    default boolean matches(String filter) {
        return TopicMatcher.matches(filter, getValue());
    }
}
