package com.alpha.mongodb.sharding.core.callback;

import com.alpha.mongodb.sharding.core.hint.ShardingHint;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ObjectUtils;
import org.bson.Document;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.core.ResolvableType;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

@NoArgsConstructor
public class HintResolutionCallbacks {

    private final Set<HintResolutionCallback> hintResolutionCallbacks = new HashSet<>();

    private final Map<Class, HintResolutionCallback> entityClassHintResolutionCallbackCache = new HashMap<>();

    public HintResolutionCallbacks(Set<HintResolutionCallback<?>> callbacks) {
        Set<HintResolutionCallback<?>> hintResolutionCallbacks = ObjectUtils.getIfNull(callbacks, Collections::emptySet);
        this.hintResolutionCallbacks.addAll(hintResolutionCallbacks);
    }

    public void discover(Set<HintResolutionCallback<?>> callbacks) {
        Set<HintResolutionCallback<?>> hintResolutionCallbacks = ObjectUtils.getIfNull(callbacks, Collections::emptySet);
        this.hintResolutionCallbacks.addAll(hintResolutionCallbacks);
    }

    public void discover(BeanFactory beanFactory) {
        beanFactory.getBeanProvider(HintResolutionCallback.class).stream().forEach(hintResolutionCallbacks::add);
    }

    public void discover(HintResolutionCallback<?> callback) {
        discover(Collections.singleton(callback));
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    public <T> Optional<HintResolutionCallback<T>> getCallback(Class<T> entityClass) {
        entityClassHintResolutionCallbackCache.computeIfAbsent(entityClass, entityKlass -> {
            for (HintResolutionCallback<?> callback : hintResolutionCallbacks) {
                ResolvableType[] callbackResolvableTypeArr = ResolvableType.forClass(callback.getClass()).getInterfaces();
                Optional<ResolvableType> callbackResolvableType = Arrays.stream(callbackResolvableTypeArr).filter(
                                resolvableType -> Objects.equals(resolvableType.resolve(), HintResolutionCallback.class) ||
                                        Objects.equals(resolvableType.resolve(), CollectionShardedEntityHintResolutionCallback.class) ||
                                        Objects.equals(resolvableType.resolve(), DatabaseShardedEntityHintResolutionCallback.class) ||
                                        Objects.equals(resolvableType.resolve(), CompositeShardedEntityHintResolutionCallback.class))
                        .findFirst();

                if (Objects.equals(ResolvableType.forClass(entityClass).resolve(),
                        callbackResolvableType.get().getGeneric(0).resolve())) {
                    return callback;
                }
            }
            return null;
        });
        return Optional.ofNullable((HintResolutionCallback<T>) entityClassHintResolutionCallbackCache.get(entityClass));
    }

    public <T> Optional<ShardingHint> callbackForFindContext(Class<T> entityClass, Query query) {
        Optional<HintResolutionCallback<T>> hintResolutionCallback = getCallback(entityClass);
        return hintResolutionCallback.map(tHintResolutionCallback -> tHintResolutionCallback.resolveHintForFindContext(query, entityClass));
    }

    public <T> Optional<ShardingHint> callbackForFindContext(Class<T> entityClass, Document query) {
        Optional<HintResolutionCallback<T>> hintResolutionCallback = getCallback(entityClass);
        return hintResolutionCallback.map(tHintResolutionCallback -> tHintResolutionCallback.resolveHintForFindContext(query, entityClass));
    }

    public <T> Optional<ShardingHint> callbackForSaveContext(Class<T> entityClass, T entity) {
        Optional<HintResolutionCallback<T>> hintResolutionCallback = getCallback(entityClass);
        return hintResolutionCallback.map(tHintResolutionCallback -> tHintResolutionCallback.resolveHintForSaveContext(entity));
    }

    public <T> Optional<ShardingHint> callbackForUpdateContext(Class<T> entityClass, Query query, UpdateDefinition updateDefinition) {
        Optional<HintResolutionCallback<T>> hintResolutionCallback = getCallback(entityClass);
        return hintResolutionCallback.map(tHintResolutionCallback -> tHintResolutionCallback.resolveHintForUpdateContext(query, updateDefinition, entityClass));
    }

    public <T> Optional<ShardingHint> callbackForUpdateContext(Class<T> entityClass, Document query, UpdateDefinition updateDefinition) {
        Optional<HintResolutionCallback<T>> hintResolutionCallback = getCallback(entityClass);
        return hintResolutionCallback.map(tHintResolutionCallback -> tHintResolutionCallback.resolveHintForUpdateContext(query, updateDefinition, entityClass));
    }

    public <T> Optional<ShardingHint> callbackForDeleteContext(Class<T> entityClass, Query query) {
        Optional<HintResolutionCallback<T>> hintResolutionCallback = getCallback(entityClass);
        return hintResolutionCallback.map(tHintResolutionCallback -> tHintResolutionCallback.resolveHintForDeleteContext(query, entityClass));
    }

    public <T> Optional<ShardingHint> callbackForDeleteContext(Class<T> entityClass, Document query) {
        Optional<HintResolutionCallback<T>> hintResolutionCallback = getCallback(entityClass);
        return hintResolutionCallback.map(tHintResolutionCallback -> tHintResolutionCallback.resolveHintForDeleteContext(query, entityClass));
    }

    public <T> Optional<ShardingHint> callbackForDeleteContext(T entity) {
        Optional<HintResolutionCallback<T>> hintResolutionCallback = getCallback((Class<T>) entity.getClass());
        return hintResolutionCallback.map(tHintResolutionCallback -> tHintResolutionCallback.resolveHintForDeleteContext(entity));
    }
}
