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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import static org.jetlinks.reactor.mqtt.MqttConstants.Topic.LEVEL_SEPARATOR;

/**
 * 预解析的 MQTT 主题，使用 Caffeine 缓存避免重复 split
 *
 * <p>对于高频主题（如 "sensor/temp"），避免每次操作都重新 split</p>
 *
 * <h3>极致内存与性能优化</h3>
 * <ul>
 *   <li>Caffeine 软引用缓存，内存压力时自动回收，避免 OOM</li>
 *   <li>预计算哈希值，加速 Map 查找</li>
 *   <li>零拷贝数组访问（调用方承诺不修改）</li>
 *   <li><b>字符串去重池</b>：层级字符串自动去重，/org/1/device 和 /org/2/device 共享 "org" 和 "device"</li>
 *   <li><b>软引用机制</b>：字符串池使用软引用，内存不足时自动释放</li>
 * </ul>
 *
 * @author PengyuDeng
 */
public final class ParsedTopic implements Topic {

    /**
     * Caffeine 软引用缓存，内存压力时自动回收
     */
    private static final Cache<String, ParsedTopic> CACHE = Caffeine.newBuilder()
                                                                    .softValues()
                                                                    .build();

    /**
     * 字符串去重池 - 常见的层级字符串（如 "device", "sensor", "org"）会被共享
     * 使用软引用，内存压力时自动回收
     */
    private static final Cache<String, String> STRING_POOL = Caffeine.newBuilder()
                                                                     .softValues()
                                                                     .build();

    private final String original;
    private final String[] levels;
    private final int hashCode;

    private ParsedTopic(String topic) {
        this.original = topic;

        String[] rawLevels = topic.split(LEVEL_SEPARATOR);
        this.levels = new String[rawLevels.length];

        for (int i = 0; i < rawLevels.length; i++) {
            this.levels[i] = internLevel(rawLevels[i]);
        }

        this.hashCode = topic.hashCode();
    }

    /**
     * 字符串去重 - 将层级字符串放入池中共享
     *
     * <p>优化目标：</p>
     * <ul>
     *   <li>/org/1/device 和 /org/2/device 共享 "org" 和 "device"</li>
     *   <li>使用软引用，内存压力时自动回收</li>
     * </ul>
     */
    private static String internLevel(String level) {
        if (level.length() <= 8) {
            return STRING_POOL.get(level, k -> k);
        }
        return level;
    }

    /**
     * 解析主题字符串（使用 Caffeine 缓存）
     *
     * @param topic 主题字符串
     * @return 解析后的主题对象
     */
    static ParsedTopic parse(String topic) {
        return CACHE.get(topic, ParsedTopic::new);
    }


    @Override
    public String[] getLevels() {
        return levels;
    }

    @Override
    public String getValue() {
        return original;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ParsedTopic other)) {
            return false;
        }
        return original.equals(other.original);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return original;
    }
}
