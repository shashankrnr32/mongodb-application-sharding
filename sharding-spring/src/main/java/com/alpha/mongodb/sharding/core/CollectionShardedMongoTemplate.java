package com.alpha.mongodb.sharding.core;

import com.alpha.mongodb.sharding.core.assitant.CollectionShardingAssistant;
import com.alpha.mongodb.sharding.core.configuration.CollectionShardingOptions;
import com.alpha.mongodb.sharding.core.exception.UnresolvableCollectionShardException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.apache.commons.collections.CollectionUtils;
import org.bson.Document;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.CursorPreparer;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.MongoWriter;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.lang.Nullable;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.Collection;
import java.util.List;

/**
 * Collection Sharded Mongo Template. To be used for collections with different names within a single
 * MongoTemplate
 *
 * @author Shashank Sharma
 */
public class CollectionShardedMongoTemplate extends ShardedMongoTemplate implements CollectionShardingAssistant {

    private static final String ID_KEY = "_id";

    public CollectionShardedMongoTemplate(MongoClient mongoClient, String databaseName, CollectionShardingOptions shardingOptions) {
        super(mongoClient, databaseName, shardingOptions);
    }

    public CollectionShardedMongoTemplate(MongoDatabaseFactory mongoDbFactory, CollectionShardingOptions shardingOptions) {
        super(mongoDbFactory, shardingOptions);
    }

    public CollectionShardedMongoTemplate(MongoDatabaseFactory mongoDbFactory, MongoConverter mongoConverter, CollectionShardingOptions shardingOptions) {
        super(mongoDbFactory, mongoConverter, shardingOptions);
    }

    @Override
    protected <T> DeleteResult doRemove(String collectionName, Query query, Class<T> entityClass, boolean multi) {
        return super.doRemove(resolveCollectionNameForDeleteContext(collectionName, entityClass, query), query, entityClass, multi);
    }

    @Override
    protected <T> T doInsert(String collectionName, T objectToSave, MongoWriter<T> writer) {
        return super.doInsert(resolveCollectionNameForSaveContext(collectionName, objectToSave), objectToSave, writer);
    }

    @Override
    protected <T> Collection<T> doInsertBatch(String collectionName, Collection<? extends T> batchToSave, MongoWriter<T> writer) {
        T firstEntity = (T) CollectionUtils.get(batchToSave, 0);
        String resolvedCollectionName = resolveCollectionNameForSaveContext(collectionName, firstEntity);
        return super.doInsertBatch(resolvedCollectionName, batchToSave, writer);
    }

    @Override
    protected <T> T doSave(String collectionName, T objectToSave, MongoWriter<T> writer) {
        return super.doSave(resolveCollectionNameForSaveContext(collectionName, objectToSave), objectToSave, writer);
    }

    @Override
    protected <T> T doFindOne(String collectionName, Document query, Document fields, CursorPreparer preparer, Class<T> entityClass) {
        return super.doFindOne(resolveCollectionNameForFindContext(collectionName, entityClass, query), query, fields, preparer, entityClass);
    }

    @Override
    protected <T> List<T> doFind(String collectionName, Document query, Document fields, Class<T> entityClass, CursorPreparer preparer) {
        return super.doFind(resolveCollectionNameForFindContext(collectionName, entityClass, query), query, fields, entityClass, preparer);
    }

    @Override
    protected <T> T doFindAndRemove(String collectionName, Document query, Document fields, Document sort, Collation collation, Class<T> entityClass) {
        return super.doFindAndRemove(resolveCollectionNameForDeleteContext(collectionName, entityClass, query), query, fields, sort, collation, entityClass);
    }

    @Override
    protected <T> List<T> doFindAndDelete(String collectionName, Query query, Class<T> entityClass) {
        List<T> result = find(query, entityClass, collectionName);

        if (!org.springframework.util.CollectionUtils.isEmpty(result)) {
            MongoPersistentEntity<T> persistentEntity =
                    (MongoPersistentEntity<T>) this.getConverter().getMappingContext().getPersistentEntity(entityClass);

            MultiValueMap<String, Object> byIds = new LinkedMultiValueMap<>();
            result.forEach(resultEntry -> {
                assert persistentEntity != null;
                byIds.add(ID_KEY, persistentEntity.getPropertyAccessor(resultEntry).getProperty(
                        persistentEntity.getIdProperty()));
            });

            Criteria[] criterias = byIds.entrySet().stream() //
                    .map(it -> Criteria.where(it.getKey()).in(it.getValue())) //
                    .toArray(Criteria[]::new);

            remove(new Query(criterias.length == 1 ? criterias[0] : new Criteria().orOperator(criterias)),
                    entityClass, collectionName);
        }

        return result;
    }

    @Override
    protected UpdateResult doUpdate(String collectionName, Query query, UpdateDefinition update, Class<?> entityClass, boolean upsert, boolean multi) {
        return super.doUpdate(resolveCollectionNameForUpdateContext(collectionName, entityClass, query, update), query, update, entityClass, upsert, multi);
    }

    public long countFromAll(Query query, Class<?> entityClass) {
        return ((CollectionShardingOptions) this.getShardingOptions()).getDefaultCollectionHintsSet().stream().mapToLong(shardHint -> count(query, entityClass)).sum();
    }

    public long countFromAll(Query query, String collectionName) {
        return ((CollectionShardingOptions) this.getShardingOptions()).getDefaultCollectionHintsSet().stream().mapToLong(shardHint -> count(query, collectionName)).sum();
    }

    public long countFromAll(Query query, @Nullable Class<?> entityClass, String collectionName) {
        return ((CollectionShardingOptions) this.getShardingOptions()).getDefaultCollectionHintsSet().stream().mapToLong(shardHint -> count(query, entityClass, collectionName)).sum();
    }

    @Override
    public long estimatedCountFromAllShards(String collectionName) {
        return ((CollectionShardingOptions) this.getShardingOptions()).getDefaultCollectionHintsSet().stream().mapToLong(shardHint -> estimatedCount(collectionName)).sum();
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
    public MongoCollection<Document> getCollection(String collectionName) {
        return super.getCollection(resolveCollectionNameWithoutEntityContext(collectionName));
    }

    @Override
    public boolean collectionExists(String collectionName) {
        return super.collectionExists(resolveCollectionNameWithoutEntityContext(collectionName));
    }

    public boolean collectionExists(String collectionName, final String collectionHint) {
        return super.collectionExists(getShardingOptions().resolveCollectionName(collectionName, collectionHint));
    }

    @Override
    protected MongoCollection<Document> doCreateCollection(String collectionName, Document collectionOptions) {
        return super.doCreateCollection(resolveCollectionNameWithoutEntityContext(collectionName), collectionOptions);
    }

    @Override
    public String resolveCollectionNameWithoutEntityContext(String collectionName) throws UnresolvableCollectionShardException {
        String hint = resolveCollectionHintWithoutEntityContext();
        validateCollectionHint(collectionName, hint);
        return getShardingOptions().resolveCollectionName(collectionName, hint);
    }

}
