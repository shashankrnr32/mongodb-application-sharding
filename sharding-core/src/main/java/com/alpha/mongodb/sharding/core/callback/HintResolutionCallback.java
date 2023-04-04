package com.alpha.mongodb.sharding.core.callback;

import com.alpha.mongodb.sharding.core.hint.ShardingHint;
import org.bson.Document;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.lang.Nullable;

public interface HintResolutionCallback<T> {

    ShardingHint resolveHintForFindContext(Document query, Class<T> entityClass);

    ShardingHint resolveHintForSaveContext(T entity);

    default ShardingHint resolveHintForFindContext(Query query, Class<T> entityClass) {
        return resolveHintForFindContext(query.getQueryObject(), entityClass);
    }

    default ShardingHint resolveHintForUpdateContext(Query query, UpdateDefinition updateDefinition, Class<T> entityClass) {
        return resolveHintForFindContext(query.getQueryObject(), entityClass);
    }

    default ShardingHint resolveHintForUpdateContext(Document query, UpdateDefinition updateDefinition, Class<T> entityClass) {
        return resolveHintForFindContext(query, entityClass);
    }

    default ShardingHint resolveHintForDeleteContext(Query query, @Nullable Class<T> entityClass) {
        return resolveHintForDeleteContext(query.getQueryObject(), entityClass);
    }

    default ShardingHint resolveHintForDeleteContext(Document query, @Nullable Class<T> entityClass) {
        return resolveHintForFindContext(query, entityClass);
    }

    default ShardingHint resolveHintForDeleteContext(T entity) {
        return resolveHintForSaveContext(entity);
    }
}
