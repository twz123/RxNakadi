package org.zalando.rxnakadi.inject.guice;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.MoreObjects.firstNonNull;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.zalando.rxnakadi.NakadiTopic;
import org.zalando.rxnakadi.NakadiTopicFactory;
import org.zalando.rxnakadi.TopicDescriptor;
import org.zalando.rxnakadi.domain.EventType;
import org.zalando.rxnakadi.domain.NakadiEvent;
import org.zalando.rxnakadi.inject.NakadiEventType;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import com.google.inject.binder.ScopedBindingBuilder;
import com.google.inject.internal.Annotations;
import com.google.inject.internal.Errors;

public final class NakadiTopicBinder {

    private static final Logger LOG = LoggerFactory.getLogger(NakadiTopicBinder.class);

    private final List<BiConsumer<Binder, Context>> bindings = new ArrayList<>();

    private final Function<Binder, Provider<? extends NakadiTopicFactory>> factoryLookup;
    private Function<Annotation[], Annotation[]> qualifierFilter;

    private static final class State {
        final Function<Binder, Provider<? extends NakadiTopicFactory>> factoryLookup;
        final Function<Annotation[], Annotation[]> qualifierFilter;
        final Object source;

        State(final NakadiTopicBinder topicBinder) {
            this.factoryLookup = topicBinder.factoryLookup;
            this.qualifierFilter = topicBinder.qualifierFilter;
            this.source = captureSource();
        }

        Context newContext() {
            return new Context(this, source);
        }

        Provider<? extends NakadiTopicFactory> getFactoryProvider(final Binder binder) {
            return factoryLookup.apply(binder.skipSources(getClass()));
        }

        Annotation[] filterQualifiers(final Annotation[] annotations) {
            return qualifierFilter == null ? annotations : qualifierFilter.apply(annotations);
        }

        private static Object captureSource() {
            final StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
            final int len = stackTrace.length;
            return len > 0 ? stackTrace[Math.min(len - 1, 4)] : null;
        }
    }

    private static final class Context {
        final State state;
        final Errors errors;

        Context(final State state, final Object source) {
            this.state = state;
            this.errors = source == null ? new Errors() : new Errors(source);
        }
    }

    private NakadiTopicBinder(final Function<Binder, Provider<? extends NakadiTopicFactory>> factoryLookup) {
        this.factoryLookup = requireNonNull(factoryLookup);
    }

    public static NakadiTopicBinder bindTopics() {
        return bindTopics(Key.get(NakadiTopicFactory.class));
    }

    public static NakadiTopicBinder bindTopics(final Key<? extends NakadiTopicFactory> factoryKey) {
        requireNonNull(factoryKey);
        return new NakadiTopicBinder(new Function<Binder, Provider<? extends NakadiTopicFactory>>() {
                    @Override
                    public Provider<? extends NakadiTopicFactory> apply(final Binder binder) {
                        return binder.skipSources(getClass()).getProvider(factoryKey);
                    }
                });
    }

    public static NakadiTopicBinder bindTopics(final Provider<? extends NakadiTopicFactory> provider) {
        requireNonNull(provider);
        return new NakadiTopicBinder(binder -> provider);
    }

    public static NakadiTopicBinder bindTopics(final NakadiTopicFactory factory) {
        requireNonNull(factory);
        return new NakadiTopicBinder(binder -> (() -> factory));
    }

    @SafeVarargs
    public final NakadiTopicBinder ignoringQualifiers(final Class<? extends Annotation>... qualifierAnntotations) {
        final Set<Class<? extends Annotation>> ignored = ImmutableSet.copyOf(qualifierAnntotations);
        this.qualifierFilter =
            qualifiers ->
                Stream.of(qualifiers)                                                     //
                      .filter(qualifier -> !ignored.contains(qualifier.annotationType())) //
                      .toArray(Annotation[]::new);
        return this;
    }

    public NakadiTopicBinder from(final Class<?> sourceClass) {
        requireNonNull(sourceClass);
        bindings.add((binder, context) -> bindFromClass(binder, context, sourceClass));
        return this;
    }

    public Module asModule() {
        final Iterable<BiConsumer<Binder, Context>> bindings = ImmutableList.copyOf(this.bindings);
        final State state = new State(this);
        return binder -> to(bindings, binder, state);
    }

    public void to(final Binder binder) {
        to(bindings, binder, new State(this));
    }

    private static void to(final Iterable<BiConsumer<Binder, Context>> bindings, final Binder binder,
            final State state) {
        final Context context = state.newContext();
        bindings.forEach(binding -> binding.accept(binder, context));
        context.errors.throwConfigurationExceptionIfErrorsExist();
    }

    private static void bindFromClass(final Binder binder, final Context context, final Class<?> sourceClass) {
        Provider<? extends NakadiTopicFactory> factoryProvider = null;
        Provider<?> eventTypeProvider = null;

        final Errors classErrors = context.errors.withSource(sourceClass);

        boolean somethingWasBound = false;
        for (final Method method : sourceClass.getMethods()) {
            final NakadiEventType eventAnnotation = method.getAnnotation(NakadiEventType.class);
            if (eventAnnotation == null) {
                LOG.trace("Skipping method [{}] since it's not annotated.", method);
                continue;
            }

            if (!somethingWasBound) {
                somethingWasBound = true;
                factoryProvider = context.state.getFactoryProvider(binder);
                eventTypeProvider = binder.getProvider(sourceClass);
            }

            bindMethod(binder, context, classErrors.withSource(method), method, eventAnnotation.value(),
                factoryProvider, eventTypeProvider);
        }

        if (!somethingWasBound) {
            LOG.warn("Nothing was bound for [{}], consider removing the binding at {}.", sourceClass.getName(),
                context.state.source);
        }
    }

    private static <E extends NakadiEvent> void bindMethod(final Binder binder, final Context ctx, final Errors errors,
            final Method method, final Class<E> eventClass,
            final Provider<? extends NakadiTopicFactory> factoryProvider, final Provider<?> sourceProvider) {

        if (method.getParameters().length > 0) {
            errors.addMessage("Cannot use a method that accepts parameters: %s.",
                Joiner.on(", ").join(method.getParameters()));
        }

        final Annotation[] annotations = method.getAnnotations();
        final Key<NakadiTopic<E>> key = getTopicKey(errors, eventClass, method,
                ctx.state.filterQualifiers(annotations));
        final Class<? extends Annotation> scope = Annotations.findScopeAnnotation(errors, annotations);
        final Function<Object, EventType> eventTypeConverter = returnTypeConverterFor(errors, method.getReturnType());

        LOG.debug("Binding topic provided by [{}] to [{}] in [{}].", method, key, scope);

        final ScopedBindingBuilder builder = binder.bind(key).toProvider(() -> {
                final EventType eventType = eventTypeConverter.apply(invoke(method, sourceProvider.get()));
                return factoryProvider.get().create(TopicDescriptor.ofType(eventClass).from(eventType).build());
            });

        if (scope != null) {
            builder.in(scope);
        }
    }

    private static <E extends NakadiEvent> Key<NakadiTopic<E>> getTopicKey(final Errors errors,
            final Class<E> eventClass, final Member member, final Annotation[] annotations) {
        final TypeLiteral<NakadiTopic<E>> topicLiteral = topicLiteralOf(eventClass);
        final Annotation bindingAnnotation = Annotations.findBindingAnnotation(errors, member, annotations);
        return bindingAnnotation == null ? Key.get(topicLiteral) : Key.get(topicLiteral, bindingAnnotation);
    }

    private static Function<Object, EventType> returnTypeConverterFor(final Errors errors, final Class<?> returnType) {
        if (EventType.class.isAssignableFrom(returnType)) {
            return value -> (EventType) value;
        }

        if (CharSequence.class.isAssignableFrom(returnType)) {
            return value -> EventType.of(Objects.toString(value, null));
        }

        errors.addMessage("Don't know how to convert %s to %s.", returnType, EventType.class.getName());
        return null;
    }

    @SuppressWarnings("serial")
    private static <E extends NakadiEvent> TypeLiteral<NakadiTopic<E>> topicLiteralOf(final Class<E> eventClass) {
        return toLiteral(new TypeToken<NakadiTopic<E>>() { }.where(new TypeParameter<E>() { }, eventClass));
    }

    @SuppressWarnings("unchecked")
    private static <E> TypeLiteral<E> toLiteral(final TypeToken<E> token) {
        return (TypeLiteral<E>) TypeLiteral.get(token.getType());
    }

    private static Object invoke(final Method method, final Object target) {
        try {
            return Proxy.isProxyClass(target.getClass())
                ? Proxy.getInvocationHandler(target).invoke(method, method, new Object[0]) //
                : method.invoke(target);
        } catch (final InvocationTargetException e) {
            final Throwable cause = firstNonNull(e.getCause(), e);
            Throwables.throwIfUnchecked(cause);
            throw new RuntimeException(cause);
        } catch (final Throwable e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
