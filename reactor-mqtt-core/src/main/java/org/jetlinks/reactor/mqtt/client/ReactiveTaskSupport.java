package org.jetlinks.reactor.mqtt.client;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

final class ReactiveTaskSupport {

    private static final BiConsumer<Publisher<Void>, Subscriber<? super Void>> TASK_STARTER = Publisher::subscribe;

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

    static ManagedTask create(Runnable onComplete, Consumer<Throwable> onError) {
        return new ManagedTask(onComplete, onError);
    }

    static ManagedTask start(Mono<Void> task, Runnable onComplete, Consumer<Throwable> onError) {
        ManagedTask managedTask = create(onComplete, onError);
        managedTask.start(task);
        return managedTask;
    }

    static final class ManagedTask extends BaseSubscriber<Void> {

        private final Runnable onComplete;
        private final Consumer<Throwable> onError;

        private ManagedTask(Runnable onComplete, Consumer<Throwable> onError) {
            this.onComplete = onComplete;
            this.onError = onError;
        }

        void start(Mono<Void> task) {
            TASK_STARTER.accept(task, this);
        }

        @Override
        protected void hookOnSubscribe(Subscription subscription) {
            requestUnbounded();
        }

        @Override
        protected void hookOnComplete() {
            if (onComplete != null) {
                onComplete.run();
            }
        }

        @Override
        protected void hookOnError(Throwable throwable) {
            if (onError != null) {
                onError.accept(throwable);
            }
        }
    }
}
