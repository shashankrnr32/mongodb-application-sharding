package com.alpha.mongodb.sharding.core.assitant;

import com.alpha.mongodb.sharding.core.entity.DatabaseShardedEntity;
import com.alpha.mongodb.sharding.core.exception.UnresolvableDatabaseShardException;
import com.alpha.mongodb.sharding.core.hint.ShardingHint;
import org.bson.Document;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;

import java.util.Map;
import java.util.Optional;

public interface DatabaseShardingAssistant<M> extends ShardingAssistant {

    Map<String, M> getDelegatedShardedMongoTemplateMap();

    default <T> M getDelegatedTemplateForFindContext(Class<T> entityClass, Query query) {
        Optional<ShardingHint> shardingHint = getHintResolutionCallbacks().callbackForFindContext(entityClass, query);
        return shardingHint.map(hint -> Optional.ofNullable(getDelegatedShardedMongoTemplateMap().get(hint.getDatabaseHint()))
                .orElseThrow(UnresolvableDatabaseShardException::new)).orElseGet(this::getDelegatedTemplateWithoutEntityContext);
    }

    default <T> M getDelegatedTemplateForFindContext(Class<T> entityClass, Document query) {
        Optional<ShardingHint> shardingHint = getHintResolutionCallbacks().callbackForFindContext(entityClass, query);
        return shardingHint.map(hint -> Optional.ofNullable(getDelegatedShardedMongoTemplateMap().get(hint.getDatabaseHint()))
                .orElseThrow(UnresolvableDatabaseShardException::new)).orElseGet(this::getDelegatedTemplateWithoutEntityContext);
    }

    default <T> M getDelegatedTemplateForDeleteContext(Class<T> entityClass, Query query) {
        Optional<ShardingHint> shardingHint = getHintResolutionCallbacks().callbackForDeleteContext(entityClass, query);
        return shardingHint.map(hint -> Optional.ofNullable(getDelegatedShardedMongoTemplateMap().get(hint.getDatabaseHint()))
                .orElseThrow(UnresolvableDatabaseShardException::new)).orElseGet(this::getDelegatedTemplateWithoutEntityContext);
    }

    default <T> M getDelegatedTemplateForDeleteContext(Class<T> entityClass, Document query) {
        Optional<ShardingHint> shardingHint = getHintResolutionCallbacks().callbackForDeleteContext(entityClass, query);
        return shardingHint.map(hint -> Optional.ofNullable(getDelegatedShardedMongoTemplateMap().get(hint.getDatabaseHint()))
                .orElseThrow(UnresolvableDatabaseShardException::new)).orElseGet(this::getDelegatedTemplateWithoutEntityContext);
    }

    default <T> M getDelegatedTemplateForDeleteContext(T entity) {
        Optional<ShardingHint> shardingHint = getHintResolutionCallbacks().callbackForDeleteContext(entity);
        return shardingHint.map(hint -> Optional.ofNullable(getDelegatedShardedMongoTemplateMap().get(hint.getDatabaseHint()))
                .orElseThrow(UnresolvableDatabaseShardException::new)).orElseGet(() -> getDelegatedTemplateWithEntityContext(entity));
    }

    default <T> M getDelegatedTemplateForUpdateContext(Class<T> entityClass, Query query, UpdateDefinition updateDefinition) {
        Optional<ShardingHint> shardingHint = getHintResolutionCallbacks().callbackForUpdateContext(entityClass, query, updateDefinition);
        return shardingHint.map(hint -> Optional.ofNullable(getDelegatedShardedMongoTemplateMap().get(hint.getDatabaseHint()))
                .orElseThrow(UnresolvableDatabaseShardException::new)).orElseGet(this::getDelegatedTemplateWithoutEntityContext);
    }

    default <T> M getDelegatedTemplateForUpdateContext(Class<T> entityClass, Document query, UpdateDefinition updateDefinition) {
        Optional<ShardingHint> shardingHint = getHintResolutionCallbacks().callbackForUpdateContext(entityClass, query, updateDefinition);
        return shardingHint.map(hint -> Optional.ofNullable(getDelegatedShardedMongoTemplateMap().get(hint.getDatabaseHint()))
                .orElseThrow(UnresolvableDatabaseShardException::new)).orElseGet(this::getDelegatedTemplateWithoutEntityContext);
    }

    default <T> M getDelegatedTemplateForSaveContext(T entity) {
        Optional<ShardingHint> shardingHint = getHintResolutionCallbacks().callbackForSaveContext((Class<T>) entity.getClass(), entity);
        return shardingHint.map(hint -> Optional.ofNullable(getDelegatedShardedMongoTemplateMap().get(hint.getDatabaseHint()))
                .orElseThrow(UnresolvableDatabaseShardException::new)).orElseGet(() -> getDelegatedTemplateWithEntityContext(entity));
    }

    default M getDelegatedTemplateWithoutEntityContext() {
        if (getDelegatedShardedMongoTemplateMap().containsKey(resolveDatabaseHintWithoutEntityContext())) {
            return getDelegatedShardedMongoTemplateMap().get(resolveDatabaseHintWithoutEntityContext());
        } else {
            throw new UnresolvableDatabaseShardException();
        }
    }

    default <T> M getDelegatedTemplateWithEntityContext(T entity) {
        if (entity instanceof DatabaseShardedEntity) {
            return getDelegatedShardedMongoTemplateMap().get(((DatabaseShardedEntity) entity).resolveDatabaseHint());
        } else {
            return getDelegatedTemplateWithoutEntityContext();
        }
    }

    default <T> String resolveDatabaseHintWithEntityContext(T entity) {
        if (entity instanceof DatabaseShardedEntity) {
            return ((DatabaseShardedEntity) entity).resolveDatabaseHint();
        } else {
            return resolveDatabaseHintWithoutEntityContext();
        }
    }
}
