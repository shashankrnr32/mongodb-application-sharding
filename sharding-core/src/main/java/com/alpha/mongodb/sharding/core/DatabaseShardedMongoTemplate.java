package com.alpha.mongodb.sharding.core;

import com.alpha.mongodb.sharding.core.configuration.CompositeShardingOptions;
import com.alpha.mongodb.sharding.core.configuration.DatabaseShardingOptions;
import com.alpha.mongodb.sharding.core.entity.DatabaseShardedEntity;
import com.alpha.mongodb.sharding.core.exception.UnresolvableDatabaseShardException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import lombok.AccessLevel;
import lombok.Getter;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.FindAndReplaceOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.util.CloseableIterator;

import java.util.*;

/**
 * Database Sharded Mongo Template. To be used for collections with same
 * names across multiple database shards
 *
 * @author Shashank Sharma
 */
public class DatabaseShardedMongoTemplate extends ShardedMongoTemplate {

    @Getter(value = AccessLevel.PROTECTED)
    private final Map<String, MongoTemplate> delegatedShardedMongoTemplateMap = new HashMap<>();

    public DatabaseShardedMongoTemplate(MongoClient mongoClient, String databaseName, DatabaseShardingOptions shardingOptions) {
        super(mongoClient, shardingOptions.resolveDatabaseName(databaseName, shardingOptions.getDefaultDatabaseHint()), shardingOptions);
        shardingOptions.getDefaultDatabaseHintsSet().forEach(shardHint -> {
            if (shardingOptions instanceof CompositeShardingOptions) {
                delegatedShardedMongoTemplateMap.put(shardHint, new CollectionShardedMongoTemplate(
                        new SimpleMongoClientDatabaseFactory(mongoClient, databaseName),
                        ((CompositeShardingOptions) shardingOptions).getDelegatedCollectionShardingOptions()));
            } else {
                delegatedShardedMongoTemplateMap.put(shardHint, new MongoTemplate(new SimpleMongoClientDatabaseFactory(mongoClient, databaseName), null));
            }
        });
    }

    public DatabaseShardedMongoTemplate(Map<String, MongoDatabaseFactory> delegatedDatabaseFactory, DatabaseShardingOptions shardingOptions) {
        super(delegatedDatabaseFactory.get(shardingOptions.getDefaultDatabaseHint()), shardingOptions);
        shardingOptions.getDefaultDatabaseHintsSet().forEach(shardHint -> {
            if (shardingOptions instanceof CompositeShardingOptions) {
                delegatedShardedMongoTemplateMap.put(shardHint, new CollectionShardedMongoTemplate(
                        delegatedDatabaseFactory.get(shardHint),
                        ((CompositeShardingOptions) shardingOptions).getDelegatedCollectionShardingOptions()));
            } else {
                delegatedShardedMongoTemplateMap.put(shardHint, new MongoTemplate(delegatedDatabaseFactory.get(shardHint), null));
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
                delegatedShardedMongoTemplateMap.put(shardHint, new MongoTemplate(delegatedDatabaseFactory.get(shardHint), mongoConverter));
            }

        });
    }

    @Override
    public <T> List<T> find(Query query, Class<T> entityClass) {
        return getDelegatedTemplateWithoutEntityContext().find(query, entityClass);
    }

    @Override
    public <T> List<T> find(Query query, Class<T> entityClass, String collectionName) {
        return getDelegatedTemplateWithoutEntityContext().find(query, entityClass, collectionName);
    }

    @Override
    public <T> List<T> findAll(Class<T> entityClass) {
        return getDelegatedTemplateWithoutEntityContext().findAll(entityClass);
    }

    @Override
    public <T> List<T> findAll(Class<T> entityClass, String collectionName) {
        return getDelegatedTemplateWithoutEntityContext().findAll(entityClass, collectionName);
    }

    @Override
    public <T> List<T> findAllAndRemove(Query query, Class<T> entityClass) {
        return getDelegatedTemplateWithoutEntityContext().findAllAndRemove(query, entityClass);
    }

    @Override
    public <T> List<T> findAllAndRemove(Query query, String collectionName) {
        return getDelegatedTemplateWithoutEntityContext().findAllAndRemove(query, collectionName);
    }

    @Override
    public <T> List<T> findAllAndRemove(Query query, Class<T> entityClass, String collectionName) {
        return getDelegatedTemplateWithoutEntityContext().findAllAndRemove(query, entityClass, collectionName);
    }

    @Override
    public <T> T findAndModify(Query query, UpdateDefinition update, Class<T> entityClass) {
        return getDelegatedTemplateWithoutEntityContext().findAndModify(query, update, entityClass);
    }

    @Override
    public <T> T findAndModify(Query query, UpdateDefinition update, Class<T> entityClass, String collectionName) {
        return getDelegatedTemplateWithoutEntityContext().findAndModify(query, update, entityClass, collectionName);
    }

    @Override
    public <T> T findAndModify(Query query, UpdateDefinition update, FindAndModifyOptions options, Class<T> entityClass) {
        return getDelegatedTemplateWithoutEntityContext().findAndModify(query, update, options, entityClass);
    }

    @Override
    public <T> T findAndModify(Query query, UpdateDefinition update, FindAndModifyOptions options, Class<T> entityClass, String collectionName) {
        return getDelegatedTemplateWithoutEntityContext().findAndModify(query, update, options, entityClass, collectionName);
    }

    @Override
    public <T> T findAndRemove(Query query, Class<T> entityClass) {
        return getDelegatedTemplateWithoutEntityContext().findAndRemove(query, entityClass);
    }

    @Override
    public <T> T findAndRemove(Query query, Class<T> entityClass, String collectionName) {
        return getDelegatedTemplateWithoutEntityContext().findAndRemove(query, entityClass, collectionName);
    }

    @Override
    public <T> T findAndReplace(Query query, T replacement) {
        return getDelegatedTemplateWithEntityContext(replacement).findAndReplace(query, replacement);
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
        return getDelegatedTemplateWithoutEntityContext().findOne(query, entityClass);
    }

    @Override
    public <T> T findOne(Query query, Class<T> entityClass, String collectionName) {
        return getDelegatedTemplateWithoutEntityContext().findOne(query, entityClass, collectionName);
    }

    @Override
    public <T> ExecutableFind<T> query(Class<T> domainType) {
        return getDelegatedTemplateWithoutEntityContext().query(domainType);
    }

    @Override
    public <T> T insert(T objectToSave) {
        return getDelegatedTemplateWithEntityContext(objectToSave).insert(objectToSave);
    }

    @Override
    public <T> ExecutableInsert<T> insert(Class<T> domainType) {
        return getDelegatedTemplateWithoutEntityContext().insert(domainType);
    }

    @Override
    public <T> T insert(T objectToSave, String collectionName) {
        return getDelegatedTemplateWithEntityContext(objectToSave).insert(objectToSave, collectionName);
    }

    @Override
    public <T> Collection<T> insert(Collection<? extends T> batchToSave, Class<?> entityClass) {
        Map<String, List<T>> dividedBatch = new HashMap<>();

        for (T entity : batchToSave) {
            String hint = resolveDatabaseHintWithEntityContext(entity);
            if (!dividedBatch.containsKey(hint)) {
                dividedBatch.put(hint, new ArrayList<>());
            }
            dividedBatch.get(hint).add(entity);
        }

        List<T> insertResult = new ArrayList<>();

        for (Map.Entry<String, List<T>> entry : dividedBatch.entrySet()) {
            insertResult.addAll(delegatedShardedMongoTemplateMap.get(entry.getKey()).insert(entry.getValue(), entityClass));
        }

        return insertResult;
    }

    @Override
    public <T> Collection<T> insert(Collection<? extends T> batchToSave, String collectionName) {
        Map<String, List<T>> dividedBatch = new HashMap<>();

        for (T entity : batchToSave) {
            String hint = resolveDatabaseHintWithEntityContext(entity);
            if (!dividedBatch.containsKey(hint)) {
                dividedBatch.put(hint, new ArrayList<>());
            }
            dividedBatch.get(hint).add(entity);
        }

        List<T> insertResult = new ArrayList<>();

        for (Map.Entry<String, List<T>> entry : dividedBatch.entrySet()) {
            insertResult.addAll(delegatedShardedMongoTemplateMap.get(entry.getKey()).insert(entry.getValue(), collectionName));
        }

        return insertResult;
    }

    @Override
    public <T> Collection<T> insertAll(Collection<? extends T> objectsToSave) {
        Map<String, List<T>> dividedBatch = new HashMap<>();

        for (T entity : objectsToSave) {
            String hint = resolveDatabaseHintWithEntityContext(entity);
            if (!dividedBatch.containsKey(hint)) {
                dividedBatch.put(hint, new ArrayList<>());
            }
            dividedBatch.get(hint).add(entity);
        }

        List<T> insertResult = new ArrayList<>();

        for (Map.Entry<String, List<T>> entry : dividedBatch.entrySet()) {
            insertResult.addAll(delegatedShardedMongoTemplateMap.get(entry.getKey()).insertAll(entry.getValue()));
        }

        return insertResult;
    }

    @Override
    public <T> ExecutableUpdate<T> update(Class<T> domainType) {
        return getDelegatedTemplateWithoutEntityContext().update(domainType);
    }

    @Override
    public UpdateResult updateFirst(Query query, UpdateDefinition update, Class<?> entityClass) {
        return getDelegatedTemplateWithoutEntityContext().updateFirst(query, update, entityClass);
    }

    @Override
    public UpdateResult updateFirst(Query query, UpdateDefinition update, String collectionName) {
        return getDelegatedTemplateWithoutEntityContext().updateFirst(query, update, collectionName);
    }

    @Override
    public UpdateResult updateFirst(Query query, UpdateDefinition update, Class<?> entityClass, String collectionName) {
        return getDelegatedTemplateWithoutEntityContext().updateFirst(query, update, entityClass, collectionName);
    }

    @Override
    public UpdateResult updateMulti(Query query, UpdateDefinition update, Class<?> entityClass) {
        return getDelegatedTemplateWithoutEntityContext().updateMulti(query, update, entityClass);
    }

    @Override
    public UpdateResult updateMulti(Query query, UpdateDefinition update, String collectionName) {
        return getDelegatedTemplateWithoutEntityContext().updateMulti(query, update, collectionName);
    }

    @Override
    public UpdateResult updateMulti(Query query, UpdateDefinition update, Class<?> entityClass, String collectionName) {
        return getDelegatedTemplateWithoutEntityContext().updateMulti(query, update, entityClass, collectionName);
    }

    @Override
    public UpdateResult upsert(Query query, UpdateDefinition update, Class<?> entityClass) {
        return getDelegatedTemplateWithoutEntityContext().upsert(query, update, entityClass);
    }

    @Override
    public UpdateResult upsert(Query query, UpdateDefinition update, String collectionName) {
        return getDelegatedTemplateWithoutEntityContext().upsert(query, update, collectionName);
    }

    @Override
    public UpdateResult upsert(Query query, UpdateDefinition update, Class<?> entityClass, String collectionName) {
        return getDelegatedTemplateWithoutEntityContext().upsert(query, update, entityClass, collectionName);
    }

    @Override
    public DeleteResult remove(Object object) {
        return getDelegatedTemplateWithEntityContext(object).remove(object);
    }

    @Override
    public DeleteResult remove(Object object, String collectionName) {
        return getDelegatedTemplateWithEntityContext(object).remove(object, collectionName);
    }

    @Override
    public DeleteResult remove(Query query, Class<?> entityClass) {
        return getDelegatedTemplateWithoutEntityContext().remove(query, entityClass);
    }

    @Override
    public DeleteResult remove(Query query, String collectionName) {
        return getDelegatedTemplateWithoutEntityContext().remove(query, collectionName);
    }

    @Override
    public DeleteResult remove(Query query, Class<?> entityClass, String collectionName) {
        return getDelegatedTemplateWithoutEntityContext().remove(query, entityClass, collectionName);
    }

    @Override
    public <T> ExecutableRemove<T> remove(Class<T> domainType) {
        return getDelegatedTemplateWithoutEntityContext().remove(domainType);
    }

    @Override
    public <T> T save(T objectToSave) {
        return getDelegatedTemplateWithEntityContext(objectToSave).save(objectToSave);
    }

    @Override
    public <T> T save(T objectToSave, String collectionName) {
        return getDelegatedTemplateWithoutEntityContext().save(objectToSave, collectionName);
    }

    @Override
    public <T> CloseableIterator<T> stream(Query query, Class<T> entityType) {
        return getDelegatedTemplateWithoutEntityContext().stream(query, entityType);
    }

    @Override
    public <T> CloseableIterator<T> stream(Query query, Class<T> entityType, String collectionName) {
        return getDelegatedTemplateWithoutEntityContext().stream(query, entityType, collectionName);
    }

    @Override
    public <T> T findById(Object id, Class<T> entityClass) {
        return getDelegatedTemplateWithoutEntityContext().findById(id, entityClass);
    }

    @Override
    public <T> T findById(Object id, Class<T> entityClass, String collectionName) {
        return getDelegatedTemplateWithoutEntityContext().findById(id, entityClass, collectionName);
    }

    @Override
    public <T> List<T> findDistinct(String field, Class<?> entityClass, Class<T> resultClass) {
        return getDelegatedTemplateWithoutEntityContext().findDistinct(field, entityClass, resultClass);
    }

    @Override
    public <T> List<T> findDistinct(Query query, String field, Class<?> entityClass, Class<T> resultClass) {
        return getDelegatedTemplateWithoutEntityContext().findDistinct(query, field, entityClass, resultClass);
    }

    @Override
    public <T> List<T> findDistinct(Query query, String field, String collection, Class<T> resultClass) {
        return getDelegatedTemplateWithoutEntityContext().findDistinct(query, field, collection, resultClass);
    }

    @Override
    public <T> List<T> findDistinct(Query query, String field, String collectionName, Class<?> entityClass, Class<T> resultClass) {
        return getDelegatedTemplateWithoutEntityContext().findDistinct(query, field, collectionName, entityClass, resultClass);
    }

    @Override
    public long countFromAll(Query query, Class<?> entityClass) {
        return this.delegatedShardedMongoTemplateMap.values().stream().mapToLong(mongoTemplate -> mongoTemplate.count(query, entityClass)).sum();
    }

    @Override
    public long countFromAll(Query query, String collectionName) {
        return this.delegatedShardedMongoTemplateMap.values().stream().mapToLong(mongoTemplate -> mongoTemplate.count(query, collectionName)).sum();
    }

    @Override
    public long countFromAll(Query query, Class<?> entityClass, String collectionName) {
        return this.delegatedShardedMongoTemplateMap.values().stream().mapToLong(mongoTemplate -> mongoTemplate.count(query, entityClass, collectionName)).sum();
    }

    @Override
    public long estimatedCountFromAllShards(String collectionName) {
        return 0;
    }

    private MongoTemplate getDelegatedTemplateWithoutEntityContext() {
        if (delegatedShardedMongoTemplateMap.containsKey(resolveDatabaseHintWithoutEntityContext())) {
            return this.delegatedShardedMongoTemplateMap.get(resolveDatabaseHintWithoutEntityContext());
        } else {
            throw new UnresolvableDatabaseShardException();
        }

    }

    private <T> MongoTemplate getDelegatedTemplateWithEntityContext(T entity) {
        if (entity instanceof DatabaseShardedEntity) {
            return delegatedShardedMongoTemplateMap.get(((DatabaseShardedEntity) entity).resolveDatabaseHint());
        } else {
            return getDelegatedTemplateWithoutEntityContext();
        }
    }

    private <T> String resolveDatabaseHintWithEntityContext(T entity) {
        if (entity instanceof DatabaseShardedEntity) {
            return ((DatabaseShardedEntity) entity).resolveDatabaseHint();
        } else {
            return resolveDatabaseHintWithoutEntityContext();
        }
    }

}
