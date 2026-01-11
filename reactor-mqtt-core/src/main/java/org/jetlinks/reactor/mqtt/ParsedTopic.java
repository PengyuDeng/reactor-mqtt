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

import java.util.concurrent.ConcurrentHashMap;

/**
 * 预解析的 MQTT 主题，使用 Caffeine 缓存避免重复 split
 *
 * <p>对于高频主题（如 "sensor/temp"），避免每次操作都重新 split</p>
 *
 * <h3>极致内存与性能优化</h3>
 * <ul>
 *   <li>Caffeine LRU 缓存，高性能无锁设计，自动淘汰最少使用的主题</li>
 *   <li>预计算哈希值，加速 Map 查找</li>
 *   <li>零拷贝数组访问（调用方承诺不修改）</li>
 *   <li><b>字符串去重池</b>：层级字符串自动去重，/org/1/device 和 /org/2/device 共享 "org" 和 "device"</li>
 * </ul>
 *
 * @author PengyuDeng
 */
public final class ParsedTopic {

    private static final int DEFAULT_CACHE_SIZE = 1024;

    /**
     * Caffeine 高性能 LRU 缓存（无锁设计，优于 LinkedHashMap）
     */
    private static final Cache<String, ParsedTopic> CACHE = Caffeine.newBuilder()
            .maximumSize(DEFAULT_CACHE_SIZE)
            .build();

    /**
     * 字符串去重池 - 常见的层级字符串（如 "device", "sensor", "org"）会被共享
     */
    private static final ConcurrentHashMap<String, String> STRING_POOL = new ConcurrentHashMap<>(256);

    private final String original;
    private final String[] levels;
    private final int hashCode;

    private ParsedTopic(String topic) {
        this.original = topic;

        String[] rawLevels = topic.split("/");
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
     *   <li>使用自定义池，可控内存占用</li>
     * </ul>
     */
    private static String internLevel(String level) {
        if (level.length() <= 8) {
            return STRING_POOL.computeIfAbsent(level, k -> k);
        }
        return level;
    }

    /**
     * 解析主题字符串（使用 Caffeine 缓存）
     *
     * @param topic 主题字符串
     * @return 解析后的主题对象
     */
    public static ParsedTopic parse(String topic) {
        return CACHE.get(topic, ParsedTopic::new);
    }

    /**
     * 获取主题层级数组（零拷贝，只读，请勿修改！）
     *
     * <p><b>警告</b>：此方法返回内部数组引用以提升性能，
     * 调用方必须承诺不修改返回的数组。
     * 如果需要修改，请自行拷贝：{@code Arrays.copyOf(levels, levels.length)}</p>
     */
    public String[] getLevels() {
        return levels;
    }

    /**
     * 获取原始主题字符串
     */
    public String getOriginal() {
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

    /**
     * 获取缓存大小
     */
    public static long getCacheSize() {
        return CACHE.estimatedSize();
    }

    /**
     * 获取字符串池大小
     */
    public static int getStringPoolSize() {
        return STRING_POOL.size();
    }
}
