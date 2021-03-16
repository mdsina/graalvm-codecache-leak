package com.github.mdsina.graalvm.codecackeleak;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyArray;
import org.graalvm.polyglot.proxy.ProxyObject;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.ContextView;

public class Main {

    public static class ContextFactory {

        private final Engine engine;
        private final HostAccess hostAccess;

        public ContextFactory() {
            this.engine = Engine.create();
            this.hostAccess = HostAccess.newBuilder(HostAccess.ALL)
                .build();
        }

        public Context createContext() {
            return Context.newBuilder("js")
                .allowExperimentalOptions(true)
                .allowAllAccess(true)
                .allowHostAccess(hostAccess)
                .option("js.experimental-foreign-object-prototype", "true")
                .engine(engine)
                .build();
        }
    }

    public static class LeakApp {

        public static final String SCRIPT_EXECUTED_KEY = "scriptExecuted";
        public static final String SCRIPT_COMPLETED = "scriptCompleted";
        public static final String REQUEST_CONTAINS_ERROR = "requestContainsError";
        public static final String CURRENT_SCHEDULER = "currentScheduler";
        public static final String JSON_PARSE = "jsonParse";

        private static final String ACTUAL_CONTEXT_KEY = "actualContext";

        private final ContextFactory contextFactory;
        private final Scheduler jsCallScheduler;
        private final Map<String, Source> sourcesCache = new ConcurrentHashMap<>(10);
        private final ObjectMapper objectMapper = new ObjectMapper();

        public LeakApp() {
            this.contextFactory = new ContextFactory();

            IntStream.range(0, 10).forEach(i -> this.sourcesCache.put(
                "test" + i,
                Source.create("js", "async function main(args) {\n" +
                    "  var res = await util.createAndSchedule(args.I);\n" +
                    "  return {O: res};\n" +
                    "}")
            ));

            this.jsCallScheduler = Schedulers.newBoundedElastic(
                200,
                10000,
                "jsCallsBounded",
                30,
                true
            );
        }

        public Mono<Map> executeRandom() {
            return Mono.fromCallable(() -> {
                Random random = new Random();
                int i = random.nextInt(10);
                return sourcesCache.get("test" + i);
            }).zipWith(Mono.fromCallable(() -> {
                Random random = new Random();
                return random.nextInt(10);
            })).flatMap(t -> execute(t.getT1(), t.getT2()));
        }

        public Mono<Map> execute(Source source, Object input) {
            return Mono
                .deferContextual(ctx -> Mono.create(sink -> {
                    Context context = ctx.get("ctx");

                    AtomicBoolean scriptExecuted = ctx.get(SCRIPT_EXECUTED_KEY);
                    Value jsonParse = ctx.get(JSON_PARSE);

                    // At this point, context fully lifted with all required values, like request's span-id and etc
                    AtomicReference<ContextView> contextAtomicReference = ctx.get(ACTUAL_CONTEXT_KEY);
                    contextAtomicReference.compareAndSet(null, ctx);

                    try {
                        context.eval(source);

                        Value promise = context.getBindings("js")
                            .getMember("main")
                            .execute(jsonParse.execute(objectMapper.writeValueAsString(Map.of("I", input))));

                        Function<Object, Object> errorHandler = createErrorHandler(sink);

                        promise
                            // `Function` interface, because Promise accepts callbacks that returns something
                            // otherwise if no value passed, promise will never execute that callback
                            .getMember("then").execute((Function<Object, Object>) o -> {
                                sink.success(o == null ? Map.of() : o);
                                return null;
                            }, errorHandler)
                            .getMember("catch").execute(errorHandler)
                            .getMember("then").executeVoid((Function<Object, Object>) o -> {
                                //TODO: may be problem if we have multiple promises that run simultaneously in background
                                // custom queue for promises required https://github.com/graalvm/graaljs/issues/210
                                System.out.println("Context closing. #"  + Thread.currentThread().getName());

                                // Wait until script completed or we deserialize all guest values to host values
                                waitingForScriptCompletion(ctx);

                                try {
                                    context.close();
                                    System.out.println("Context closed. #" + Thread.currentThread().getName());
                                } catch (Throwable e) {
                                    System.out.printf("Error occurred on expected Context closing: %s", e.getMessage());
                                    e.printStackTrace();
                                    throw e;
                                }
                                return null;
                            });
                    } catch (Exception e) {
                        System.out.printf("CTX_ERROR: %s\n", e.getMessage());
                        e.printStackTrace();
                        sink.error(e);
                    } finally {
                        // Make sure that promises will be resolved after context leaving to avoid concurrent access
                        while (!scriptExecuted.compareAndSet(false, true)) {}
                    }
                }))
                .flatMap(res -> Mono.deferContextual(ctx -> {
                    markCompleted(ctx);
                    return Mono.just((Map) res);
                }))
                // Thread changed on subscribe() in ThenableUtil.toThenableAndSchedule
                // Back to main, because thread will be released
                .publishOn(jsCallScheduler)
                .flatMap(r -> Mono.deferContextual(ctx -> {

                    releaseThread(ctx);
                    return Mono.just(r);
                }))
                .timeout(Duration.ofSeconds(5))
                .onErrorResume(e -> Mono.deferContextual(ctx -> {
                    Context context = ctx.get("ctx");

                    releaseThread(ctx);
                    markCompleted(ctx);

                    if (e instanceof TimeoutException) {
                        context.close(true);
                        return Mono.error(new RuntimeException("Timeout"));
                    }

                    return Mono.error(e);
                }))
                .contextWrite(ctx -> {
                    // Trying to avoid errors when multiple threads accessing Polyglot Context
                    // Polyglot receive all threads that accessing Context and think that there are concurrent case,
                    // but is doesn't
                    // So, most of Context accessors will be executed only on specific thread
                    Scheduler jsScheduler = Schedulers.newSingle("js");
                    jsScheduler.start();

                    var actualContextReference = new AtomicReference<ContextView>(null);

                    Context context = contextFactory.createContext();
                    context.getBindings("js")
                        .putMember("util", new PromiseScheduler(actualContextReference));

                    return ctx
                        .put("ctx", context)
                        .put(ACTUAL_CONTEXT_KEY, actualContextReference)
                        .put(CURRENT_SCHEDULER, jsScheduler)
                        .put(SCRIPT_EXECUTED_KEY, new AtomicBoolean(false))
                        .put(SCRIPT_COMPLETED, new CountDownLatch(1))
                        .put(REQUEST_CONTAINS_ERROR, new AtomicBoolean(false))
                        .put(JSON_PARSE, context.getBindings("js").getMember("JSON").getMember("parse"));
                });
        }

        private Function<Object, Object> createErrorHandler(MonoSink<?> sink) {
            return o -> {
                Throwable error = new RuntimeException("{}");
                if (o != null) {
                    var v = Value.asValue(o);
                    if (v.isHostObject()) {
                        error = v.asHostObject();
                    } else if (v.isProxyObject()) {
                        error = v.asProxyObject();
                    } else if (o instanceof Throwable) {
                        error = (Throwable) o;
                    } else if (v.hasMember("message") && v.hasMember("name")) {
                        // an error has 2 fields: name + message
                        String nameField = v.getMember("name").asString();
                        String messageField = v.getMember("message").asString();
                        // empty message fields usually it JS prints the name field
                        error = new Throwable("".equals(messageField) ? nameField : messageField);
                    } else {
                        error = new Throwable(o.toString());
                    }
                }
                sink.error(error);
                return null;
            };
        }

        private void waitingForScriptCompletion(ContextView ctx) {
            CountDownLatch scriptCompletedLatch = ctx.get(SCRIPT_COMPLETED);
            try {
                boolean await = scriptCompletedLatch.await(10, TimeUnit.SECONDS);
                if (!await) {
                    throw new RuntimeException("Await ended with " + 10);
                }
            } catch (InterruptedException e) {
                //ignore
            }
        }

        private void releaseThread(ContextView ctx) {
            Scheduler scheduler = ctx.get(CURRENT_SCHEDULER);
            if (!scheduler.isDisposed()) {
                scheduler.dispose();
            }
        }

        private void markCompleted(ContextView ctx) {
            CountDownLatch scriptCompletedLatch = ctx.get(SCRIPT_COMPLETED);
            scriptCompletedLatch.countDown();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        var app = new LeakApp();
        var latch = new CountDownLatch(5);
        app.executeRandom()
            .delayElement(Duration.ofMillis(50))
            .repeat(10000L)
            .doOnComplete(latch::countDown)
            .subscribe();

        app.executeRandom()
            .delayElement(Duration.ofMillis(50))
            .repeat(10000L)
            .doOnComplete(latch::countDown)
            .subscribe();

        app.executeRandom()
            .delayElement(Duration.ofMillis(50))
            .repeat(10000L)
            .doOnComplete(latch::countDown)
            .subscribe();

        app.executeRandom()
            .delayElement(Duration.ofMillis(50))
            .repeat(10000L)
            .doOnComplete(latch::countDown)
            .subscribe();

        app.executeRandom()
            .delayElement(Duration.ofMillis(50))
            .repeat(10000L)
            .doOnComplete(latch::countDown)
            .subscribe();

        latch.await();
    }

    public static class PromiseScheduler {

        private final AtomicReference<ContextView> contextAtomicReference;

        public PromiseScheduler(AtomicReference<ContextView> contextAtomicReference) {
            this.contextAtomicReference = contextAtomicReference;
        }

        public Thenable createAndSchedule(Object input) {
            return ThenableUtil.toThenableAndSchedule(
                contextAtomicReference.get(),
                Mono.delay(Duration.ofMillis((new Random()).nextInt(30) + 1))
                    .flatMap(d -> Mono.fromCallable(() -> InteropUtils.javaObjToGuestObj(Map.of(
                        "status", 200,
                        "statusText", "text",
                        "headers", InteropUtils.javaObjToGuestObj(Map.of()),
                        "data", InteropUtils.javaObjToGuestObj(Map.of(
                            "data", "blabla",
                            "input", input
                        ))
                    ))).subscribeOn(Schedulers.boundedElastic()))
            );
        }
    }

    @FunctionalInterface
    public interface Thenable {

        void then(Value resolve, Value reject);
    }

    public static class ThenableUtil {

        public static Thenable toThenableAndSchedule(ContextView context, Mono<?> value) {
            return (resolve, reject) ->
                value
                    .publishOn(context.get(LeakApp.CURRENT_SCHEDULER))
                    .flatMap(v -> waitForMainThread(context).thenReturn(v))
                    .doOnNext(resolve::executeVoid)
                    .onErrorResume(e -> waitForMainThread(context).then(Mono.error(e)))
                    .doOnError(reject::executeVoid)
                    .contextWrite(context)
                    .subscribe();
        }

        private static Mono<Void> waitForMainThread(ContextView context) {
            return Mono.fromRunnable(() -> {
                System.out.println("wait for synchronization #" + Thread.currentThread().getName());

                AtomicBoolean isExecuted = context.get(LeakApp.SCRIPT_EXECUTED_KEY);
                // wait until `main.then()` call completed on main thread to avoid concurrent access to js context
                while (!isExecuted.compareAndSet(true, true)) {}

                System.out.println("synchronized #" + Thread.currentThread().getName());
            }).then();
        }
    }

    public static class InteropUtils {

        public static Object javaObjToGuestObj(Object value) {
            if (value instanceof Map) {
                return fromMap((Map) value);
            }
            return value;
        }

        // Copy of ProxyObject.fromMap
        // Can return deep proxy objects, instead of flat raw values
        // A bit simplified for this example
        private static ProxyObject fromMap(Map<String, Object> values) {
            return new ProxyObject() {

                public void putMember(String key, Value value) {
                    values.put(key, value.isHostObject() ? value.asHostObject() : value);
                }

                public boolean hasMember(String key) {
                    return values.containsKey(key);
                }

                public Object getMemberKeys() {
                    return new ProxyArray() {
                        private final Object[] keys = values.keySet().toArray();

                        public void set(long index, Value value) {
                            throw new UnsupportedOperationException();
                        }

                        public long getSize() {
                            return keys.length;
                        }

                        public Object get(long index) {
                            if (index < 0 || index > Integer.MAX_VALUE) {
                                throw new ArrayIndexOutOfBoundsException();
                            }
                            return keys[(int) index];
                        }
                    };
                }

                public Object getMember(String key) {
                    return javaObjToGuestObj(values.get(key));
                }
            };
        }
    }
}
