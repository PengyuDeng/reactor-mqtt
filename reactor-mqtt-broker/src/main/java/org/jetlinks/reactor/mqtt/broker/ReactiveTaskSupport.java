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
            Mono<Void> task = Mono.defer(() -> {
                Mono<Void> actual = mapper.apply(value);
                return actual != null ? actual : Mono.empty();
            });
            if (tasks == null) {
                tasks = new ArrayList<>();
            }
            tasks.add(task);
        }
        if (tasks == null || tasks.isEmpty()) {
            return Mono.empty();
        }
        return Mono.whenDelayError(tasks.toArray(Mono[]::new));
    }
}
