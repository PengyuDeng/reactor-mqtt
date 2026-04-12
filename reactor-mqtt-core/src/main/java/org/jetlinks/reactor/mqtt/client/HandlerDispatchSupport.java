package org.jetlinks.reactor.mqtt.client;

import reactor.core.publisher.Mono;

import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

final class HandlerDispatchSupport {

    private HandlerDispatchSupport() {
    }

    static <T> Mono<Void> dispatch(Iterable<Function<T, Mono<Void>>> handlers,
                                   T value,
                                   String topic,
                                   Logger log) {
        return ReactiveTaskSupport.whenAll(
                handlers,
                handler -> safelyHandle(handler, value, topic, log)
        );
    }

    private static <T> Mono<Void> safelyHandle(Function<T, Mono<Void>> handler,
                                               T value,
                                               String topic,
                                               Logger log) {
        return Mono.defer(() -> handler.apply(value))
                      .onErrorResume(error -> {
                          log.log(Level.WARNING, error,
                                  () -> String.format("Handler error for topic [%s]: %s",
                                                      topic, error.getMessage()));
                          return Mono.empty();
                      });
    }
}
