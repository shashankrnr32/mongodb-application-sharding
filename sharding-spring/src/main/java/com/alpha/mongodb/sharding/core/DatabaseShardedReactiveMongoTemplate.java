package com.alpha.mongodb.sharding.core;

import com.alpha.mongodb.sharding.core.assitant.DatabaseShardingAssistant;
import com.alpha.mongodb.sharding.core.configuration.CompositeShardingOptions;
import com.alpha.mongodb.sharding.core.configuration.DatabaseShardingOptions;
import com.alpha.mongodb.sharding.core.exception.UnresolvableDatabaseShardException;
import com.alpha.mongodb.sharding.core.hint.ShardingHint;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.MongoClient;
import lombok.Getter;
import org.bson.Document;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.FindAndReplaceOptions;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class DatabaseShardedReactiveMongoTemplate extends ShardedReactiveMongoTemplate implements DatabaseShardingAssistant<ReactiveMongoTemplate> {

    @Getter
    private final Map<String, ReactiveMongoTemplate> delegatedShardedMongoTemplateMap = new HashMap<>();

    public DatabaseShardedReactiveMongoTemplate(Map<String, MongoClient> delegatedMongoClient, String databaseName, DatabaseShardingOptions shardingOptions) {
        super(delegatedMongoClient.get(shardingOptions.getDefaultDatabaseHint()), databaseName, shardingOptions);
        shardingOptions.getDefaultDatabaseHintsSet().forEach(shardHint -> {
            if (shardingOptions instanceof CompositeShardingOptions) {
                delegatedShardedMongoTemplateMap.put(shardHint, new CollectionShardedReactiveMongoTemplate(
                        new SimpleReactiveMongoDatabaseFactory(delegatedMongoClient.get(shardHint),
                                shardingOptions.resolveDatabaseName(databaseName, shardHint)),
                        ((CompositeShardingOptions) shardingOptions).getDelegatedCollectionShardingOptions()));
            } else {
                delegatedShardedMongoTemplateMap.put(shardHint, new ReactiveMongoTemplate(
                        new SimpleReactiveMongoDatabaseFactory(delegatedMongoClient.get(shardHint), databaseName), null));
            }
        });
    }

    public DatabaseShardedReactiveMongoTemplate(MongoClient delegatedMongoClient, String databaseName, DatabaseShardingOptions shardingOptions) {
        this(shardingOptions.getDefaultDatabaseHintsSet().stream().collect(Collectors.toMap(s -> s, s -> delegatedMongoClient)), databaseName, shardingOptions);
    }

    public DatabaseShardedReactiveMongoTemplate(Map<String, ReactiveMongoDatabaseFactory> delegatedDatabaseFactory, DatabaseShardingOptions shardingOptions) {
        super(delegatedDatabaseFactory.get(shardingOptions.getDefaultDatabaseHint()), shardingOptions);
        shardingOptions.getDefaultDatabaseHintsSet().forEach(shardHint -> {
            if (shardingOptions instanceof CompositeShardingOptions) {
                delegatedShardedMongoTemplateMap.put(shardHint, new CollectionShardedReactiveMongoTemplate(
                        delegatedDatabaseFactory.get(shardHint),
                        ((CompositeShardingOptions) shardingOptions).getDelegatedCollectionShardingOptions()));
            } else {
                delegatedShardedMongoTemplateMap.put(shardHint, new ReactiveMongoTemplate(delegatedDatabaseFactory.get(shardHint), null));
            }
        });
    }

    public DatabaseShardedReactiveMongoTemplate(Map<String, ReactiveMongoDatabaseFactory> delegatedDatabaseFactory, MongoConverter mongoConverter, DatabaseShardingOptions shardingOptions) {
        super(delegatedDatabaseFactory.get(shardingOptions.getDefaultDatabaseHint()), mongoConverter, shardingOptions);
        shardingOptions.getDefaultDatabaseHintsSet().forEach(shardHint -> {
            if (shardingOptions instanceof CompositeShardingOptions) {
                delegatedShardedMongoTemplateMap.put(shardHint, new CollectionShardedReactiveMongoTemplate(
                        delegatedDatabaseFactory.get(shardHint),
                        mongoConverter,
                        ((CompositeShardingOptions) shardingOptions).getDelegatedCollectionShardingOptions()));
            } else {
                delegatedShardedMongoTemplateMap.put(shardHint, new ReactiveMongoTemplate(delegatedDatabaseFactory.get(shardHint), mongoConverter));
            }
        });
    }

    public DatabaseShardedReactiveMongoTemplate(Map<String, ReactiveMongoDatabaseFactory> delegatedDatabaseFactory, MongoConverter mongoConverter, Consumer<Throwable> subscriptionExceptionHandler, DatabaseShardingOptions shardingOptions) {
        super(delegatedDatabaseFactory.get(shardingOptions.getDefaultDatabaseHint()), mongoConverter, subscriptionExceptionHandler, shardingOptions);
        shardingOptions.getDefaultDatabaseHintsSet().forEach(shardHint -> {
            if (shardingOptions instanceof CompositeShardingOptions) {
                delegatedShardedMongoTemplateMap.put(shardHint, new CollectionShardedReactiveMongoTemplate(
                        delegatedDatabaseFactory.get(shardHint),
                        mongoConverter,
                        ((CompositeShardingOptions) shardingOptions).getDelegatedCollectionShardingOptions()));
            } else {
                delegatedShardedMongoTemplateMap.put(shardHint, new ReactiveMongoTemplate(delegatedDatabaseFactory.get(shardHint), mongoConverter));
            }
        });
    }

    @Override
    public <T> Flux<T> find(Query query, Class<T> entityClass, String collectionName) {
        return getDelegatedTemplateForFindContext(entityClass, query).find(query, entityClass, collectionName);
    }

    @Override
    public <T> Flux<T> find(Query query, Class<T> entityClass) {
        return getDelegatedTemplateForFindContext(entityClass, query).find(query, entityClass);
    }

    @Override
    public <T> Flux<T> findAll(Class<T> entityClass, String collectionName) {
        return getDelegatedTemplateForFindContext(entityClass, new Document()).findAll(entityClass, collectionName);
    }

    @Override
    public <T> Flux<T> findAll(Class<T> entityClass) {
        return getDelegatedTemplateForFindContext(entityClass, new Document()).findAll(entityClass);
    }

    @Override
    public <T> Flux<T> findAllAndRemove(Query query, Class<T> entityClass) {
        return getDelegatedTemplateForDeleteContext(entityClass, query).findAllAndRemove(query, entityClass);
    }

    @Override
    public <T> Flux<T> findAllAndRemove(Query query, String collectionName) {
        return getDelegatedTemplateWithoutEntityContext().findAllAndRemove(query, collectionName);
    }

    @Override
    public <T> Flux<T> findAllAndRemove(Query query, Class<T> entityClass, String collectionName) {
        return getDelegatedTemplateForDeleteContext(entityClass, query).findAllAndRemove(query, entityClass, collectionName);
    }

    @Override
    public <T> Mono<T> findAndModify(Query query, UpdateDefinition update, Class<T> entityClass) {
        return getDelegatedTemplateForUpdateContext(entityClass, query, update).findAndModify(query, update, entityClass);
    }

    @Override
    public <T> Mono<T> findAndModify(Query query, UpdateDefinition update, Class<T> entityClass, String collectionName) {
        return getDelegatedTemplateForUpdateContext(entityClass, query, update).findAndModify(query, update, entityClass, collectionName);
    }

    @Override
    public <T> Mono<T> findAndModify(Query query, UpdateDefinition update, FindAndModifyOptions options, Class<T> entityClass) {
        return getDelegatedTemplateForUpdateContext(entityClass, query, update).findAndModify(query, update, options, entityClass);
    }

    @Override
    public <T> Mono<T> findAndModify(Query query, UpdateDefinition update, FindAndModifyOptions options, Class<T> entityClass, String collectionName) {
        return getDelegatedTemplateForUpdateContext(entityClass, query, update).findAndModify(query, update, options, entityClass, collectionName);
    }

    @Override
    public <T> Mono<T> findAndRemove(Query query, Class<T> entityClass) {
        return getDelegatedTemplateForDeleteContext(entityClass, query).findAndRemove(query, entityClass);
    }

    @Override
    public <T> Mono<T> findAndRemove(Query query, Class<T> entityClass, String collectionName) {
        return getDelegatedTemplateForDeleteContext(entityClass, query).findAndRemove(query, entityClass, collectionName);
    }

    @Override
    public <T> Mono<T> findAndReplace(Query query, T replacement) {
        return getDelegatedTemplateForSaveContext(replacement).findAndReplace(query, replacement);
    }

    @Override
    public <T> Mono<T> findAndReplace(Query query, T replacement, String collectionName) {
        return getDelegatedTemplateWithEntityContext(replacement).findAndReplace(query, replacement, collectionName);
    }

    @Override
    public <T> Mono<T> findAndReplace(Query query, T replacement, FindAndReplaceOptions options) {
        return getDelegatedTemplateWithEntityContext(replacement).findAndReplace(query, replacement, options);
    }

    @Override
    public <T> Mono<T> findAndReplace(Query query, T replacement, FindAndReplaceOptions options, String collectionName) {
        return getDelegatedTemplateWithEntityContext(replacement).findAndReplace(query, replacement, options, collectionName);
    }

    @Override
    public <S, T> Mono<T> findAndReplace(Query query, S replacement, FindAndReplaceOptions options, Class<S> entityType, Class<T> resultType) {
        return getDelegatedTemplateWithEntityContext(replacement).findAndReplace(query, replacement, options, entityType, resultType);
    }

    @Override
    public <T> Mono<T> findAndReplace(Query query, T replacement, FindAndReplaceOptions options, Class<T> entityType, String collectionName) {
        return getDelegatedTemplateWithEntityContext(replacement).findAndReplace(query, replacement, options, entityType, collectionName);
    }

    @Override
    public <S, T> Mono<T> findAndReplace(Query query, S replacement, FindAndReplaceOptions options, Class<S> entityType, String collectionName, Class<T> resultType) {
        return getDelegatedTemplateWithEntityContext(replacement).findAndReplace(query, replacement, options, entityType, collectionName, resultType);
    }

    @Override
    public <T> Mono<T> findOne(Query query, Class<T> entityClass) {
        return getDelegatedTemplateForFindContext(entityClass, query).findOne(query, entityClass);
    }

    @Override
    public <T> Mono<T> findOne(Query query, Class<T> entityClass, String collectionName) {
        return getDelegatedTemplateForFindContext(entityClass, query).findOne(query, entityClass, collectionName);
    }

    @Override
    public <T> ReactiveFind<T> query(Class<T> domainType) {
        return getDelegatedTemplateWithoutEntityContext().query(domainType);
    }

    @Override
    public <T> Mono<T> insert(T objectToSave) {
        return getDelegatedTemplateForSaveContext(objectToSave).insert(objectToSave);
    }

    @Override
    public <T> ReactiveInsert<T> insert(Class<T> domainType) {
        return getDelegatedTemplateWithoutEntityContext().insert(domainType);
    }

    @Override
    public <T> Mono<T> insert(T objectToSave, String collectionName) {
        return getDelegatedTemplateForSaveContext(objectToSave).insert(objectToSave, collectionName);
    }

    @Override
    public <T> Flux<T> insert(Collection<? extends T> batchToSave, Class<?> entityClass) {
        Map<String, List<T>> dividedBatch = new HashMap<>();

        for (T entity : batchToSave) {
            String hint = getHintResolutionCallbacks()
                    .callbackForSaveContext((Class<T>) entity.getClass(), entity).map(ShardingHint::getDatabaseHint)
                    .orElseGet(() -> resolveDatabaseHintWithEntityContext(entity));
            dividedBatch.computeIfAbsent(hint, h -> new ArrayList<>());
            dividedBatch.get(hint).add(entity);
        }

        Flux<T> insertResult = Flux.empty();

        for (Map.Entry<String, List<T>> entry : dividedBatch.entrySet()) {
            insertResult = Flux.mergeSequential(insertResult, Optional.ofNullable(delegatedShardedMongoTemplateMap.get(entry.getKey()))
                    .orElseThrow(UnresolvableDatabaseShardException::new).insert(entry.getValue(), entityClass));
        }

        return insertResult;
    }

    @Override
    public <T> Flux<T> insert(Collection<? extends T> batchToSave, String collectionName) {
        Map<String, List<T>> dividedBatch = new HashMap<>();

        for (T entity : batchToSave) {
            String hint = getHintResolutionCallbacks()
                    .callbackForSaveContext((Class<T>) entity.getClass(), entity).map(ShardingHint::getDatabaseHint)
                    .orElseGet(() -> resolveDatabaseHintWithEntityContext(entity));
            dividedBatch.computeIfAbsent(hint, h -> new ArrayList<>());
            dividedBatch.get(hint).add(entity);
        }

        Flux<T> insertResult = Flux.empty();

        for (Map.Entry<String, List<T>> entry : dividedBatch.entrySet()) {
            insertResult = Flux.mergeSequential(insertResult, Optional.ofNullable(delegatedShardedMongoTemplateMap.get(entry.getKey()))
                    .orElseThrow(UnresolvableDatabaseShardException::new).insert(entry.getValue(), collectionName));
        }

        return insertResult;
    }

    @Override
    public <T> Flux<T> insertAll(Collection<? extends T> objectsToSave) {
        Map<String, List<T>> dividedBatch = new HashMap<>();

        for (T entity : objectsToSave) {
            String hint = getHintResolutionCallbacks()
                    .callbackForSaveContext((Class<T>) entity.getClass(), entity).map(ShardingHint::getDatabaseHint)
                    .orElseGet(() -> resolveDatabaseHintWithEntityContext(entity));
            dividedBatch.computeIfAbsent(hint, h -> new ArrayList<>());
            dividedBatch.get(hint).add(entity);
        }

        Flux<T> insertResult = Flux.empty();

        for (Map.Entry<String, List<T>> entry : dividedBatch.entrySet()) {
            insertResult = Flux.mergeSequential(insertResult, Optional.ofNullable(delegatedShardedMongoTemplateMap.get(entry.getKey()))
                    .orElseThrow(UnresolvableDatabaseShardException::new).insertAll(entry.getValue()));
        }

        return insertResult;
    }

    @Override
    public <T> ReactiveUpdate<T> update(Class<T> domainType) {
        return getDelegatedTemplateWithoutEntityContext().update(domainType);
    }

    @Override
    public Mono<UpdateResult> updateFirst(Query query, UpdateDefinition update, Class<?> entityClass) {
        return getDelegatedTemplateForUpdateContext(entityClass, query, update).updateFirst(query, update, entityClass);
    }

    @Override
    public Mono<UpdateResult> updateFirst(Query query, UpdateDefinition update, String collectionName) {
        return getDelegatedTemplateWithoutEntityContext().updateFirst(query, update, collectionName);
    }

    @Override
    public Mono<UpdateResult> updateFirst(Query query, UpdateDefinition update, Class<?> entityClass, String collectionName) {
        return getDelegatedTemplateForUpdateContext(entityClass, query, update).updateFirst(query, update, entityClass, collectionName);
    }

    @Override
    public Mono<UpdateResult> updateMulti(Query query, UpdateDefinition update, Class<?> entityClass) {
        return getDelegatedTemplateForUpdateContext(entityClass, query, update).updateMulti(query, update, entityClass);
    }

    @Override
    public Mono<UpdateResult> updateMulti(Query query, UpdateDefinition update, String collectionName) {
        return getDelegatedTemplateWithoutEntityContext().updateMulti(query, update, collectionName);
    }

    @Override
    public Mono<UpdateResult> updateMulti(Query query, UpdateDefinition update, Class<?> entityClass, String collectionName) {
        return getDelegatedTemplateForUpdateContext(entityClass, query, update).updateMulti(query, update, entityClass, collectionName);
    }

    @Override
    public Mono<UpdateResult> upsert(Query query, UpdateDefinition update, Class<?> entityClass) {
        return getDelegatedTemplateForUpdateContext(entityClass, query, update).upsert(query, update, entityClass);
    }

    @Override
    public Mono<UpdateResult> upsert(Query query, UpdateDefinition update, String collectionName) {
        return getDelegatedTemplateWithoutEntityContext().upsert(query, update, collectionName);
    }

    @Override
    public Mono<UpdateResult> upsert(Query query, UpdateDefinition update, Class<?> entityClass, String collectionName) {
        return getDelegatedTemplateForUpdateContext(entityClass, query, update).upsert(query, update, entityClass, collectionName);
    }

    @Override
    public Mono<DeleteResult> remove(Object object) {
        return getDelegatedTemplateForDeleteContext(object).remove(object);
    }

    @Override
    public Mono<DeleteResult> remove(Object object, String collectionName) {
        return getDelegatedTemplateForDeleteContext(object).remove(object, collectionName);
    }

    @Override
    public Mono<DeleteResult> remove(Query query, Class<?> entityClass) {
        return getDelegatedTemplateForDeleteContext(entityClass, query).remove(query, entityClass);
    }

    @Override
    public Mono<DeleteResult> remove(Query query, String collectionName) {
        return getDelegatedTemplateWithoutEntityContext().remove(query, collectionName);
    }

    @Override
    public Mono<DeleteResult> remove(Query query, Class<?> entityClass, String collectionName) {
        return getDelegatedTemplateForDeleteContext(entityClass, query).remove(query, entityClass, collectionName);
    }

    @Override
    public <T> ReactiveRemove<T> remove(Class<T> domainType) {
        return getDelegatedTemplateWithoutEntityContext().remove(domainType);
    }

    @Override
    public <T> Mono<T> save(T objectToSave) {
        return getDelegatedTemplateForSaveContext(objectToSave).save(objectToSave);
    }

    @Override
    public <T> Mono<T> save(T objectToSave, String collectionName) {
        return getDelegatedTemplateForSaveContext(objectToSave).save(objectToSave, collectionName);
    }

    @Override
    public <T> Mono<T> findById(Object id, Class<T> entityClass) {
        return getDelegatedTemplateForFindContext(entityClass, new Document("_id", id)).findById(id, entityClass);
    }

    @Override
    public <T> Mono<T> findById(Object id, Class<T> entityClass, String collectionName) {
        return getDelegatedTemplateForFindContext(entityClass, new Document("_id", id)).findById(id, entityClass, collectionName);
    }

    @Override
    public <T> Flux<T> findDistinct(String field, Class<?> entityClass, Class<T> resultClass) {
        return getDelegatedTemplateForFindContext(entityClass, new Query()).findDistinct(field, entityClass, resultClass);
    }

    @Override
    public <T> Flux<T> findDistinct(Query query, String field, Class<?> entityClass, Class<T> resultClass) {
        return getDelegatedTemplateForFindContext(entityClass, query).findDistinct(query, field, entityClass, resultClass);
    }

    @Override
    public <T> Flux<T> findDistinct(Query query, String field, String collection, Class<T> resultClass) {
        return getDelegatedTemplateWithoutEntityContext().findDistinct(query, field, collection, resultClass);
    }

    @Override
    public <T> Flux<T> findDistinct(Query query, String field, String collectionName, Class<?> entityClass, Class<T> resultClass) {
        return getDelegatedTemplateForFindContext(entityClass, query).findDistinct(query, field, collectionName, entityClass, resultClass);
    }

}
