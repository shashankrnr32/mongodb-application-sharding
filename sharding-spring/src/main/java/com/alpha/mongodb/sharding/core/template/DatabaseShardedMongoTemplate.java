package com.alpha.mongodb.sharding.core.template;

import com.alpha.mongodb.sharding.core.assitant.DatabaseShardingAssistant;
import com.alpha.mongodb.sharding.core.configuration.CompositeShardingOptions;
import com.alpha.mongodb.sharding.core.configuration.DatabaseShardingOptions;
import com.alpha.mongodb.sharding.core.exception.UnresolvableDatabaseShardException;
import com.alpha.mongodb.sharding.core.executable.DatabaseShardedExecutableFindSupport;
import com.alpha.mongodb.sharding.core.executable.DatabaseShardedExecutableInsertSupport;
import com.alpha.mongodb.sharding.core.executable.DatabaseShardedExecutableRemoveSupport;
import com.alpha.mongodb.sharding.core.executable.DatabaseShardedExecutableUpdateSupport;
import com.alpha.mongodb.sharding.core.hint.ShardingHint;
import com.mongodb.client.MongoClient;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import lombok.Getter;
import org.bson.Document;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.FindAndReplaceOptions;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.util.CloseableIterator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Database Sharded Reactive Mongo Template. To be used for collections with same
 * names across multiple database shards
 *
 * @author Shashank Sharma
 */
public class DatabaseShardedMongoTemplate extends ShardedMongoTemplate implements DatabaseShardingAssistant<ExtendedMongoTemplate> {

    @Getter
    private final Map<String, ExtendedMongoTemplate> delegatedShardedMongoTemplateMap = new HashMap<>();

    private static final Query ALL_QUERY = new Query();

    public DatabaseShardedMongoTemplate(Map<String, MongoClient> delegatedMongoClientMap, String databaseName, DatabaseShardingOptions shardingOptions) {
        super(delegatedMongoClientMap.get(shardingOptions.getDefaultDatabaseHint()),
                shardingOptions.resolveDatabaseName(databaseName, shardingOptions.getDefaultDatabaseHint()), shardingOptions);
        shardingOptions.getDefaultDatabaseHintsSet().forEach(shardHint -> {
            if (shardingOptions instanceof CompositeShardingOptions) {
                delegatedShardedMongoTemplateMap.put(shardHint, new CollectionShardedMongoTemplate(
                        new SimpleMongoClientDatabaseFactory(delegatedMongoClientMap.get(shardHint),
                                shardingOptions.resolveDatabaseName(databaseName, shardHint)),
                        ((CompositeShardingOptions) shardingOptions).getDelegatedCollectionShardingOptions()));
            } else {
                delegatedShardedMongoTemplateMap.put(shardHint, new ExtendedMongoTemplate(
                        new SimpleMongoClientDatabaseFactory(delegatedMongoClientMap.get(shardHint), databaseName), null));
            }
        });
    }

    public DatabaseShardedMongoTemplate(MongoClient delegatedMongoClient, String databaseName, DatabaseShardingOptions shardingOptions) {
        this(shardingOptions.getDefaultDatabaseHintsSet().stream().collect(Collectors.toMap(s -> s, s -> delegatedMongoClient)), databaseName, shardingOptions);
    }

    public DatabaseShardedMongoTemplate(Map<String, MongoDatabaseFactory> delegatedDatabaseFactory, DatabaseShardingOptions shardingOptions) {
        super(delegatedDatabaseFactory.get(shardingOptions.getDefaultDatabaseHint()), shardingOptions);
        shardingOptions.getDefaultDatabaseHintsSet().forEach(shardHint -> {
            if (shardingOptions instanceof CompositeShardingOptions) {
                delegatedShardedMongoTemplateMap.put(shardHint, new CollectionShardedMongoTemplate(
                        delegatedDatabaseFactory.get(shardHint),
                        ((CompositeShardingOptions) shardingOptions).getDelegatedCollectionShardingOptions()));
            } else {
                delegatedShardedMongoTemplateMap.put(shardHint, new ExtendedMongoTemplate(delegatedDatabaseFactory.get(shardHint), null));
            }
        });
    }

    public DatabaseShardedMongoTemplate(Map<String, MongoDatabaseFactory> delegatedDatabaseFactory, MongoConverter mongoConverter, DatabaseShardingOptions shardingOptions) {
        super(delegatedDatabaseFactory.get(shardingOptions.getDefaultDatabaseHint()), mongoConverter, shardingOptions);
        shardingOptions.getDefaultDatabaseHintsSet().forEach(shardHint -> {
            if (shardingOptions instanceof CompositeShardingOptions) {
                delegatedShardedMongoTemplateMap.put(shardHint, new CollectionShardedMongoTemplate(
                        delegatedDatabaseFactory.get(shardHint),
                        mongoConverter,
                        ((CompositeShardingOptions) shardingOptions).getDelegatedCollectionShardingOptions()));
            } else {
                delegatedShardedMongoTemplateMap.put(shardHint, new ExtendedMongoTemplate(delegatedDatabaseFactory.get(shardHint), mongoConverter));
            }

        });
    }

    @Override
    public <T> List<T> find(Query query, Class<T> entityClass) {
        return getDelegatedTemplateForFindContext(entityClass, query).find(query, entityClass);
    }

    @Override
    public <T> List<T> find(Query query, Class<T> entityClass, String collectionName) {
        return getDelegatedTemplateForFindContext(entityClass, query).find(query, entityClass, collectionName);
    }

    @Override
    public <T> List<T> findAll(Class<T> entityClass) {
        return getDelegatedTemplateForFindContext(entityClass, new Document()).findAll(entityClass);
    }

    @Override
    public <T> List<T> findAll(Class<T> entityClass, String collectionName) {
        return getDelegatedTemplateForFindContext(entityClass, new Document()).findAll(entityClass, collectionName);
    }

    @Override
    public <T> List<T> findAllAndRemove(Query query, Class<T> entityClass) {
        return getDelegatedTemplateForDeleteContext(entityClass, query).findAllAndRemove(query, entityClass);
    }

    @Override
    public <T> List<T> findAllAndRemove(Query query, String collectionName) {
        return getDelegatedTemplateWithoutEntityContext().findAllAndRemove(query, collectionName);
    }

    @Override
    public <T> List<T> findAllAndRemove(Query query, Class<T> entityClass, String collectionName) {
        return getDelegatedTemplateForDeleteContext(entityClass, query).findAllAndRemove(query, entityClass, collectionName);
    }

    @Override
    public <T> T findAndModify(Query query, UpdateDefinition update, Class<T> entityClass) {
        return getDelegatedTemplateForUpdateContext(entityClass, query, update).findAndModify(query, update, entityClass);
    }

    @Override
    public <T> T findAndModify(Query query, UpdateDefinition update, Class<T> entityClass, String collectionName) {
        return getDelegatedTemplateForUpdateContext(entityClass, query, update).findAndModify(query, update, entityClass, collectionName);
    }

    @Override
    public <T> T findAndModify(Query query, UpdateDefinition update, FindAndModifyOptions options, Class<T> entityClass) {
        return getDelegatedTemplateForUpdateContext(entityClass, query, update).findAndModify(query, update, options, entityClass);
    }

    @Override
    public <T> T findAndModify(Query query, UpdateDefinition update, FindAndModifyOptions options, Class<T> entityClass, String collectionName) {
        return getDelegatedTemplateForUpdateContext(entityClass, query, update).findAndModify(query, update, options, entityClass, collectionName);
    }

    @Override
    public <T> T findAndRemove(Query query, Class<T> entityClass) {
        return getDelegatedTemplateForDeleteContext(entityClass, query).findAndRemove(query, entityClass);
    }

    @Override
    public <T> T findAndRemove(Query query, Class<T> entityClass, String collectionName) {
        return getDelegatedTemplateForDeleteContext(entityClass, query).findAndRemove(query, entityClass, collectionName);
    }

    @Override
    public <T> T findAndReplace(Query query, T replacement) {
        return getDelegatedTemplateForSaveContext(replacement).findAndReplace(query, replacement);
    }

    @Override
    public <T> T findAndReplace(Query query, T replacement, String collectionName) {
        return getDelegatedTemplateWithEntityContext(replacement).findAndReplace(query, replacement, collectionName);
    }

    @Override
    public <T> T findAndReplace(Query query, T replacement, FindAndReplaceOptions options) {
        return getDelegatedTemplateWithEntityContext(replacement).findAndReplace(query, replacement, options);
    }

    @Override
    public <T> T findAndReplace(Query query, T replacement, FindAndReplaceOptions options, String collectionName) {
        return getDelegatedTemplateWithEntityContext(replacement).findAndReplace(query, replacement, options, collectionName);
    }

    @Override
    public <S, T> T findAndReplace(Query query, S replacement, FindAndReplaceOptions options, Class<S> entityType, Class<T> resultType) {
        return getDelegatedTemplateWithEntityContext(replacement).findAndReplace(query, replacement, options, entityType, resultType);
    }

    @Override
    public <T> T findAndReplace(Query query, T replacement, FindAndReplaceOptions options, Class<T> entityType, String collectionName) {
        return getDelegatedTemplateWithEntityContext(replacement).findAndReplace(query, replacement, options, entityType, collectionName);
    }

    @Override
    public <S, T> T findAndReplace(Query query, S replacement, FindAndReplaceOptions options, Class<S> entityType, String collectionName, Class<T> resultType) {
        return getDelegatedTemplateWithEntityContext(replacement).findAndReplace(query, replacement, options, entityType, collectionName, resultType);
    }

    @Override
    public <T> T findOne(Query query, Class<T> entityClass) {
        return getDelegatedTemplateForFindContext(entityClass, query).findOne(query, entityClass);
    }

    @Override
    public <T> T findOne(Query query, Class<T> entityClass, String collectionName) {
        return getDelegatedTemplateForFindContext(entityClass, query).findOne(query, entityClass, collectionName);
    }

    @Override
    public <T> T insert(T objectToSave) {
        return getDelegatedTemplateForSaveContext(objectToSave).insert(objectToSave);
    }

    @Override
    public <T> T insert(T objectToSave, String collectionName) {
        return getDelegatedTemplateForSaveContext(objectToSave).insert(objectToSave, collectionName);
    }

    @Override
    public <T> Collection<T> insert(Collection<? extends T> batchToSave, Class<?> entityClass) {
        Map<String, List<T>> dividedBatch = new HashMap<>();

        for (T entity : batchToSave) {
            String hint = getHintResolutionCallbacks()
                    .callbackForSaveContext((Class<T>) entity.getClass(), entity).map(ShardingHint::getDatabaseHint)
                    .orElseGet(() -> resolveDatabaseHintWithEntityContext(entity));
            dividedBatch.computeIfAbsent(hint, h -> new ArrayList<>());
            dividedBatch.get(hint).add(entity);
        }

        List<T> insertResult = new ArrayList<>();

        for (Map.Entry<String, List<T>> entry : dividedBatch.entrySet()) {
            insertResult.addAll(Optional.ofNullable(delegatedShardedMongoTemplateMap.get(entry.getKey()))
                    .orElseThrow(UnresolvableDatabaseShardException::new).insert(entry.getValue(), entityClass));
        }

        return insertResult;
    }

    @Override
    public <T> Collection<T> insert(Collection<? extends T> batchToSave, String collectionName) {
        Map<String, List<T>> dividedBatch = new HashMap<>();

        for (T entity : batchToSave) {
            String hint = getHintResolutionCallbacks()
                    .callbackForSaveContext((Class<T>) entity.getClass(), entity).map(ShardingHint::getDatabaseHint)
                    .orElseGet(() -> resolveDatabaseHintWithEntityContext(entity));
            dividedBatch.computeIfAbsent(hint, h -> new ArrayList<>());
            dividedBatch.get(hint).add(entity);
        }

        List<T> insertResult = new ArrayList<>();

        for (Map.Entry<String, List<T>> entry : dividedBatch.entrySet()) {
            insertResult.addAll(Optional.ofNullable(delegatedShardedMongoTemplateMap.get(entry.getKey()))
                    .orElseThrow(UnresolvableDatabaseShardException::new).insert(entry.getValue(), collectionName));
        }

        return insertResult;
    }

    @Override
    public <T> Collection<T> insertAll(Collection<? extends T> objectsToSave) {
        Map<String, List<T>> dividedBatch = new HashMap<>();

        for (T entity : objectsToSave) {
            String hint = getHintResolutionCallbacks()
                    .callbackForSaveContext((Class<T>) entity.getClass(), entity).map(ShardingHint::getDatabaseHint)
                    .orElseGet(() -> resolveDatabaseHintWithEntityContext(entity));
            dividedBatch.computeIfAbsent(hint, h -> new ArrayList<>());
            dividedBatch.get(hint).add(entity);
        }

        List<T> insertResult = new ArrayList<>();

        for (Map.Entry<String, List<T>> entry : dividedBatch.entrySet()) {
            insertResult.addAll(Optional.ofNullable(delegatedShardedMongoTemplateMap.get(entry.getKey()))
                    .orElseThrow(UnresolvableDatabaseShardException::new).insertAll(entry.getValue()));
        }

        return insertResult;
    }

    @Override
    public UpdateResult updateFirst(Query query, UpdateDefinition update, Class<?> entityClass) {
        return getDelegatedTemplateForUpdateContext(entityClass, query, update).updateFirst(query, update, entityClass);
    }

    @Override
    public UpdateResult updateFirst(Query query, UpdateDefinition update, String collectionName) {
        return getDelegatedTemplateWithoutEntityContext().updateFirst(query, update, collectionName);
    }

    @Override
    public UpdateResult updateFirst(Query query, UpdateDefinition update, Class<?> entityClass, String collectionName) {
        return getDelegatedTemplateForUpdateContext(entityClass, query, update).updateFirst(query, update, entityClass, collectionName);
    }

    @Override
    public UpdateResult updateMulti(Query query, UpdateDefinition update, Class<?> entityClass) {
        return getDelegatedTemplateForUpdateContext(entityClass, query, update).updateMulti(query, update, entityClass);
    }

    @Override
    public UpdateResult updateMulti(Query query, UpdateDefinition update, String collectionName) {
        return getDelegatedTemplateWithoutEntityContext().updateMulti(query, update, collectionName);
    }

    @Override
    public UpdateResult updateMulti(Query query, UpdateDefinition update, Class<?> entityClass, String collectionName) {
        return getDelegatedTemplateForUpdateContext(entityClass, query, update).updateMulti(query, update, entityClass, collectionName);
    }

    @Override
    public UpdateResult upsert(Query query, UpdateDefinition update, Class<?> entityClass) {
        return getDelegatedTemplateForUpdateContext(entityClass, query, update).upsert(query, update, entityClass);
    }

    @Override
    public UpdateResult upsert(Query query, UpdateDefinition update, String collectionName) {
        return getDelegatedTemplateWithoutEntityContext().upsert(query, update, collectionName);
    }

    @Override
    public UpdateResult upsert(Query query, UpdateDefinition update, Class<?> entityClass, String collectionName) {
        return getDelegatedTemplateForUpdateContext(entityClass, query, update).upsert(query, update, entityClass, collectionName);
    }

    @Override
    public DeleteResult remove(Object object) {
        return getDelegatedTemplateForDeleteContext(object).remove(object);
    }

    @Override
    public DeleteResult remove(Object object, String collectionName) {
        return getDelegatedTemplateForDeleteContext(object).remove(object, collectionName);
    }

    @Override
    public DeleteResult remove(Query query, Class<?> entityClass) {
        return getDelegatedTemplateForDeleteContext(entityClass, query).remove(query, entityClass);
    }

    @Override
    public DeleteResult remove(Query query, String collectionName) {
        return getDelegatedTemplateWithoutEntityContext().remove(query, collectionName);
    }

    @Override
    public DeleteResult remove(Query query, Class<?> entityClass, String collectionName) {
        return getDelegatedTemplateForDeleteContext(entityClass, query).remove(query, entityClass, collectionName);
    }

    @Override
    public <T> T save(T objectToSave) {
        return getDelegatedTemplateForSaveContext(objectToSave).save(objectToSave);
    }

    @Override
    public <T> T save(T objectToSave, String collectionName) {
        return getDelegatedTemplateForSaveContext(objectToSave).save(objectToSave, collectionName);
    }

    @Override
    public <T> CloseableIterator<T> stream(Query query, Class<T> entityType) {
        return getDelegatedTemplateForFindContext(entityType, query).stream(query, entityType);
    }

    @Override
    public <T> CloseableIterator<T> stream(Query query, Class<T> entityType, String collectionName) {
        return getDelegatedTemplateForFindContext(entityType, query).stream(query, entityType, collectionName);
    }

    @Override
    public <T> T findById(Object id, Class<T> entityClass) {
        return getDelegatedTemplateForFindContext(entityClass, new Document("_id", id)).findById(id, entityClass);
    }

    @Override
    public <T> T findById(Object id, Class<T> entityClass, String collectionName) {
        return getDelegatedTemplateForFindContext(entityClass, new Document("_id", id)).findById(id, entityClass, collectionName);
    }

    @Override
    public <T> List<T> findDistinct(String field, Class<?> entityClass, Class<T> resultClass) {
        return getDelegatedTemplateForFindContext(entityClass, new Query()).findDistinct(field, entityClass, resultClass);
    }

    @Override
    public <T> List<T> findDistinct(Query query, String field, Class<?> entityClass, Class<T> resultClass) {
        return getDelegatedTemplateForFindContext(entityClass, query).findDistinct(query, field, entityClass, resultClass);
    }

    @Override
    public <T> List<T> findDistinct(Query query, String field, String collection, Class<T> resultClass) {
        return getDelegatedTemplateWithoutEntityContext().findDistinct(query, field, collection, resultClass);
    }

    @Override
    public <T> List<T> findDistinct(Query query, String field, String collectionName, Class<?> entityClass, Class<T> resultClass) {
        return getDelegatedTemplateForFindContext(entityClass, query).findDistinct(query, field, collectionName, entityClass, resultClass);
    }

    @Override
    public <T> ExecutableFind<T> query(Class<T> domainType) {
        return new DatabaseShardedExecutableFindSupport<>(
                delegatedShardedMongoTemplateMap, domainType, domainType, null, ALL_QUERY, getHintResolutionCallbacks(),
                (DatabaseShardingOptions) getShardingOptions());
    }

    @Override
    public <T> ExecutableRemove<T> remove(Class<T> domainType) {
        return new DatabaseShardedExecutableRemoveSupport<>(
                delegatedShardedMongoTemplateMap, domainType, null, ALL_QUERY, getHintResolutionCallbacks(),
                (DatabaseShardingOptions) getShardingOptions());
    }

    @Override
    public <T> ExecutableUpdate<T> update(Class<T> domainType) {
        return new DatabaseShardedExecutableUpdateSupport<>(
                delegatedShardedMongoTemplateMap, domainType, ALL_QUERY, null, null, null, null, null, domainType,
                getHintResolutionCallbacks(), (DatabaseShardingOptions) getShardingOptions());
    }

    @Override
    public <T> ExecutableInsert<T> insert(Class<T> domainType) {
        return new DatabaseShardedExecutableInsertSupport<>(
                delegatedShardedMongoTemplateMap, domainType, null, null, getHintResolutionCallbacks(),
                (DatabaseShardingOptions) getShardingOptions());
    }
}
