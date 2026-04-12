package org.jetlinks.reactor.mqtt.broker;

import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

final class ReactiveTaskSupport {

    private ReactiveTaskSupport() {
    }

    static <T> Mono<Void> whenAll(Iterable<T> source, Function<T, Mono<Void>> mapper) {
        List<Mono<Void>> tasks = null;
        for (T value : source) {
            Mono<Void> task = mapper.apply(value);
            if (task == null) {
                continue;
            }
            if (tasks == null) {
                tasks = new ArrayList<>();
            }
            tasks.add(task);
        }
        if (tasks == null || tasks.isEmpty()) {
            return Mono.empty();
        }
        if (tasks.size() == 1) {
            return tasks.get(0);
        }
        return Mono.whenDelayError(tasks.toArray(Mono[]::new));
    }
}
