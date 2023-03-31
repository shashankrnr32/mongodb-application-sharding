package com.alpha.mongodb.sharding.core;

import com.alpha.mongodb.sharding.core.entity.MongoDBCollectionShardedEntity;
import com.alpha.mongodb.sharding.core.exception.UnresolvableCollectionShardException;
import com.alpha.mongodb.sharding.core.hint.ShardingHint;
import com.alpha.mongodb.sharding.core.hint.ShardingHintManager;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.bson.Document;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.CursorPreparer;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.MongoWriter;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Collection Sharded Mongo Template. To be used for collections with different names within a single
 * MongoTemplate
 *
 * @author Shashank Sharma
 */
public class CollectionShardedMongoTemplate extends ShardedMongoTemplate {

    @Getter
    @Setter
    private List<String> allShardHints = new ArrayList<>();

    public CollectionShardedMongoTemplate(MongoClient mongoClient, String databaseName) {
        super(mongoClient, databaseName);
    }

    public CollectionShardedMongoTemplate(MongoDatabaseFactory mongoDbFactory) {
        super(mongoDbFactory);
    }

    public CollectionShardedMongoTemplate(MongoDatabaseFactory mongoDbFactory, MongoConverter mongoConverter) {
        super(mongoDbFactory, mongoConverter);
    }

    @Override
    protected <T> DeleteResult doRemove(String collectionName, Query query, Class<T> entityClass, boolean multi) {
        return super.doRemove(resolveCollectionNameWithoutEntityContext(collectionName), query, entityClass, multi);
    }

    @Override
    protected <T> T doInsert(String collectionName, T objectToSave, MongoWriter<T> writer) {
        return super.doInsert(resolveCollectionNameWithEntityContext(collectionName, objectToSave), objectToSave, writer);
    }

    /**
     * Insert all elements into a collection.
     *
     * @param collectionName Base collection name
     * @param batchToSave    Batch to save
     * @param writer         Mongo Writer
     * @param <T>            The entity type
     * @return Collection of inserted batch
     */
    @Override
    protected <T> Collection<T> doInsertBatch(String collectionName, Collection<? extends T> batchToSave, MongoWriter<T> writer) {
        T firstEntity = (T) CollectionUtils.get(batchToSave, 0);
        String resolvedCollectionName = resolveCollectionNameWithEntityContext(collectionName, firstEntity);
        return super.doInsertBatch(resolvedCollectionName, batchToSave, writer);
    }

    @Override
    protected <T> T doSave(String collectionName, T objectToSave, MongoWriter<T> writer) {
        return super.doSave(resolveCollectionNameWithEntityContext(collectionName, objectToSave), objectToSave, writer);
    }

    @Override
    protected <T> T doFindOne(String collectionName, Document query, Document fields, CursorPreparer preparer, Class<T> entityClass) {
        return super.doFindOne(resolveCollectionNameWithoutEntityContext(collectionName), query, fields, preparer, entityClass);
    }

    @Override
    protected <T> List<T> doFind(String collectionName, Document query, Document fields, Class<T> entityClass, CursorPreparer preparer) {
        return super.doFind(resolveCollectionNameWithoutEntityContext(collectionName), query, fields, entityClass, preparer);
    }

    @Override
    protected <T> T doFindAndRemove(String collectionName, Document query, Document fields, Document sort, Collation collation, Class<T> entityClass) {
        return super.doFindAndRemove(resolveCollectionNameWithoutEntityContext(collectionName), query, fields, sort, collation, entityClass);
    }

    @Override
    protected <T> List<T> doFindAndDelete(String collectionName, Query query, Class<T> entityClass) {
        return super.doFindAndDelete(resolveCollectionNameWithoutEntityContext(collectionName), query, entityClass);
    }

    @Override
    protected UpdateResult doUpdate(String collectionName, Query query, UpdateDefinition update, Class<?> entityClass, boolean upsert, boolean multi) {
        return super.doUpdate(resolveCollectionNameWithoutEntityContext(collectionName), query, update, entityClass, upsert, multi);
    }

    public long countFromAllShards(Query query, Class<?> entityClass) {
        return this.allShardHints.stream().mapToLong(shardHint -> count(query, entityClass)).sum();
    }

    public long countFromAllShards(Query query, String collectionName) {
        return this.allShardHints.stream().mapToLong(shardHint -> count(query, collectionName)).sum();
    }

    public long countFromAllShards(Query query, @Nullable Class<?> entityClass, String collectionName) {
        return this.allShardHints.stream().mapToLong(shardHint -> count(query, entityClass, collectionName)).sum();
    }

    @Override
    public long estimatedCountFromAllShards(String collectionName) {
        return this.allShardHints.stream().mapToLong(shardHint -> estimatedCount(collectionName)).sum();
    }

    @Override
    protected long doCount(String collectionName, Document filter, CountOptions options) {
        return super.doCount(resolveCollectionNameWithoutEntityContext(collectionName), filter, options);
    }

    @Override
    protected long doEstimatedCount(String collectionName, EstimatedDocumentCountOptions options) {
        return super.doEstimatedCount(resolveCollectionNameWithoutEntityContext(collectionName), options);
    }

    @Override
    protected long doExactCount(String collectionName, Document filter, CountOptions options) {
        return super.doExactCount(resolveCollectionNameWithoutEntityContext(collectionName), filter, options);
    }

    @Override
    public MongoCollection<Document> getCollection(String collectionName) {
        return super.getCollection(resolveCollectionNameWithoutEntityContext(collectionName));
    }

    @Override
    public boolean collectionExists(String collectionName) {
        return super.collectionExists(resolveCollectionNameWithoutEntityContext(collectionName));
    }

    public boolean collectionExists(String collectionName, final String collectionHint) {
        return super.collectionExists(resolveName(collectionName, collectionHint));
    }

    @NonNull
    private <T> String resolveCollectionNameWithEntityContext(final String collectionName, final T objectToSave) {
        String resolvedCollectionName;
        if (objectToSave instanceof MongoDBCollectionShardedEntity) {
            String hint = ((MongoDBCollectionShardedEntity) objectToSave).getCollectionHint();
            resolvedCollectionName = resolveName(collectionName, hint);
        } else {
            Optional<ShardingHint> hint = ShardingHintManager.get();
            if (hint.isPresent() && null != hint.get().getCollectionHint()) {
                resolvedCollectionName = resolveName(collectionName, hint.get().getCollectionHint());
            } else {
                throw new UnresolvableCollectionShardException();
            }
        }
        return resolvedCollectionName;
    }

}
