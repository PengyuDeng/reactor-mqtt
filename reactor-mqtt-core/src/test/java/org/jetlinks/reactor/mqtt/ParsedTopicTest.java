package org.jetlinks.reactor.mqtt;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class ParsedTopicTest {

    @Test
    void shouldKeepSplitCompatibleLevelSemantics() {
        assertLevels("a/b", "a", "b");
        assertLevels("a//b", "a", "", "b");
        assertLevels("/a/b", "", "a", "b");
        assertLevels("a/b/", "a", "b");
        assertLevels("/", new String[0]);
        assertLevels("", "");
    }

    @Test
    void shouldReuseCachedParsedTopicInstance() {
        ParsedTopic first = ParsedTopic.parse("sensor/room1/temp");
        ParsedTopic second = ParsedTopic.parse("sensor/room1/temp");

        assertSame(first, second);
    }

    private void assertLevels(String topic, String... expected) {
        assertArrayEquals(expected, ParsedTopic.parse(topic).getLevels());
    }
}
