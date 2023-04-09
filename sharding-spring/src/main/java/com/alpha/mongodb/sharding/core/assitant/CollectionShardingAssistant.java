package com.alpha.mongodb.sharding.core.assitant;

import com.alpha.mongodb.sharding.core.entity.CollectionShardedEntity;
import com.alpha.mongodb.sharding.core.exception.UnresolvableCollectionShardException;
import com.alpha.mongodb.sharding.core.hint.ShardingHint;
import com.alpha.mongodb.sharding.core.hint.ShardingHintManager;
import org.bson.Document;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.lang.NonNull;

import java.util.Optional;

public interface CollectionShardingAssistant extends ShardingAssistant {

    @NonNull
    default <T> String resolveCollectionNameWithEntityContext(final String collectionName, final T entity)
            throws UnresolvableCollectionShardException {
        String resolvedCollectionName;
        if (entity instanceof CollectionShardedEntity) {
            String hint = ((CollectionShardedEntity) entity).resolveCollectionHint();
            validateCollectionHint(collectionName, hint);
            resolvedCollectionName = getShardingOptions().resolveCollectionName(collectionName, hint);
        } else {
            Optional<ShardingHint> shardingHint = ShardingHintManager.getHint();
            if (shardingHint.isPresent() && null != shardingHint.get().getCollectionHint()) {
                String hint = shardingHint.get().getCollectionHint();
                validateCollectionHint(collectionName, hint);
                resolvedCollectionName = getShardingOptions().resolveCollectionName(collectionName, hint);
            } else {
                throw new UnresolvableCollectionShardException();
            }
        }
        return resolvedCollectionName;
    }

    default void validateCollectionHint(final String collectionName, final String hint)
            throws UnresolvableCollectionShardException {
        if (!this.getShardingOptions().validateCollectionHint(collectionName, hint)) {
            throw new UnresolvableCollectionShardException();
        }
    }

    default <T> String resolveCollectionNameForFindContext(String collectionName, Class<T> entityClass, Query query) {
        Optional<ShardingHint> shardingHint = getHintResolutionCallbacks().callbackForFindContext(entityClass, query);
        return shardingHint.map(hint -> {
            validateCollectionHint(collectionName, hint.getCollectionHint());
            return getShardingOptions().resolveCollectionName(collectionName, hint.getCollectionHint());
        }).orElseGet(() -> resolveCollectionNameWithoutEntityContext(collectionName));
    }

    default <T> String resolveCollectionNameForFindContext(String collectionName, Class<T> entityClass, Document query) {
        Optional<ShardingHint> shardingHint = getHintResolutionCallbacks().callbackForFindContext(entityClass, query);
        return shardingHint.map(hint -> {
            validateCollectionHint(collectionName, hint.getCollectionHint());
            return getShardingOptions().resolveCollectionName(collectionName, hint.getCollectionHint());
        }).orElseGet(() -> resolveCollectionNameWithoutEntityContext(collectionName));
    }

    default <T> String resolveCollectionNameForSaveContext(String collectionName, T entity) {
        Optional<ShardingHint> shardingHint = getHintResolutionCallbacks().callbackForSaveContext((Class<T>) entity.getClass(), entity);
        if (shardingHint.isPresent()) {
            return getShardingOptions().resolveCollectionName(collectionName, shardingHint.get().getCollectionHint());
        } else {
            return resolveCollectionNameWithEntityContext(collectionName, entity);
        }
    }

    default <T> String resolveCollectionNameForUpdateContext(String collectionName, Class<T> entityClass, Query query, UpdateDefinition updateDefinition) {
        Optional<ShardingHint> shardingHint = getHintResolutionCallbacks().callbackForUpdateContext(entityClass, query, updateDefinition);
        return shardingHint.map(hint -> {
            validateCollectionHint(collectionName, hint.getCollectionHint());
            return getShardingOptions().resolveCollectionName(collectionName, hint.getCollectionHint());
        }).orElseGet(() -> resolveCollectionNameWithoutEntityContext(collectionName));
    }

    default <T> String resolveCollectionNameForDeleteContext(String collectionName, Class<T> entityClass, Query query) {
        Optional<ShardingHint> shardingHint = getHintResolutionCallbacks().callbackForDeleteContext(entityClass, query);
        return shardingHint.map(hint -> {
            validateCollectionHint(collectionName, hint.getCollectionHint());
            return getShardingOptions().resolveCollectionName(collectionName, hint.getCollectionHint());
        }).orElseGet(() -> resolveCollectionNameWithoutEntityContext(collectionName));
    }

    default <T> String resolveCollectionNameForDeleteContext(String collectionName, Class<T> entityClass, Document query) {
        Optional<ShardingHint> shardingHint = getHintResolutionCallbacks().callbackForDeleteContext(entityClass, query);
        return shardingHint.map(hint -> {
            validateCollectionHint(collectionName, hint.getCollectionHint());
            return getShardingOptions().resolveCollectionName(collectionName, hint.getCollectionHint());
        }).orElseGet(() -> resolveCollectionNameWithoutEntityContext(collectionName));
    }
}
