package org.jetlinks.reactor.mqtt.server;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Mono;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

final class ReactiveTaskSupport {

    private static final BiConsumer<Publisher<Void>, Subscriber<? super Void>> TASK_STARTER = Publisher::subscribe;

    private ReactiveTaskSupport() {
    }

    static void start(Mono<Void> task, Consumer<Throwable> onError) {
        ManagedTask managedTask = new ManagedTask(onError);
        managedTask.start(task);
    }

    private static final class ManagedTask extends BaseSubscriber<Void> {

        private final Consumer<Throwable> onError;

        private ManagedTask(Consumer<Throwable> onError) {
            this.onError = onError;
        }

        private void start(Mono<Void> task) {
            TASK_STARTER.accept(task, this);
        }

        @Override
        protected void hookOnSubscribe(Subscription subscription) {
            requestUnbounded();
        }

        @Override
        protected void hookOnError(Throwable throwable) {
            if (onError != null) {
                onError.accept(throwable);
            }
        }
    }
}
