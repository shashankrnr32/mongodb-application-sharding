package com.alpha.mongodb.sharding.core.template;

import com.alpha.mongodb.sharding.core.assitant.CollectionShardingAssistant;
import com.alpha.mongodb.sharding.core.configuration.CollectionShardingOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.apache.commons.collections.CollectionUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.FindPublisherPreparer;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.MongoWriter;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.function.Consumer;

/**
 * Collection Sharded Reactive Mongo Template for users who use reactive mongo template.
 *
 * @author Shashank Sharma
 * @see CollectionShardedMongoTemplate
 */
public class CollectionShardedReactiveMongoTemplate extends ShardedReactiveMongoTemplate implements CollectionShardingAssistant {
    private static final Logger LOGGER = LoggerFactory.getLogger(CollectionShardedReactiveMongoTemplate.class);

    private static final String ID_KEY = "_id";

    public CollectionShardedReactiveMongoTemplate(MongoClient mongoClient, String databaseName, CollectionShardingOptions collectionShardingOptions) {
        super(mongoClient, databaseName, collectionShardingOptions);
    }

    public CollectionShardedReactiveMongoTemplate(ReactiveMongoDatabaseFactory mongoDatabaseFactory, CollectionShardingOptions collectionShardingOptions) {
        super(mongoDatabaseFactory, collectionShardingOptions);
    }

    public CollectionShardedReactiveMongoTemplate(ReactiveMongoDatabaseFactory mongoDatabaseFactory, MongoConverter mongoConverter, CollectionShardingOptions collectionShardingOptions) {
        super(mongoDatabaseFactory, mongoConverter, collectionShardingOptions);
    }

    public CollectionShardedReactiveMongoTemplate(ReactiveMongoDatabaseFactory mongoDatabaseFactory, MongoConverter mongoConverter, Consumer<Throwable> subscriptionExceptionHandler, CollectionShardingOptions collectionShardingOptions) {
        super(mongoDatabaseFactory, mongoConverter, subscriptionExceptionHandler, collectionShardingOptions);
    }

    @Override
    protected <T> Mono<DeleteResult> doRemove(String collectionName, Query query, Class<T> entityClass) {
        return super.doRemove(resolveCollectionNameForDeleteContext(collectionName, entityClass, query), query, entityClass);
    }

    @Override
    protected <T> Mono<T> doInsert(String collectionName, T objectToSave, MongoWriter<Object> writer) {
        return super.doInsert(resolveCollectionNameForSaveContext(collectionName, objectToSave), objectToSave, writer);
    }

    @Override
    protected <T> Flux<T> doInsertBatch(String collectionName, Collection<? extends T> batchToSave, MongoWriter<Object> writer) {
        T firstEntity = (T) CollectionUtils.get(batchToSave, 0);
        String resolvedCollectionName = resolveCollectionNameForSaveContext(collectionName, firstEntity);
        return super.doInsertBatch(resolvedCollectionName, batchToSave, writer);
    }

    @Override
    protected <T> Mono<T> doSave(String collectionName, T objectToSave, MongoWriter<Object> writer) {
        return super.doSave(resolveCollectionNameForSaveContext(collectionName, objectToSave), objectToSave, writer);
    }

    @Override
    protected <T> Mono<T> doFindOne(String collectionName, Document query, Document fields, Class<T> entityClass, FindPublisherPreparer preparer) {
        return super.doFindOne(resolveCollectionNameForFindContext(collectionName, entityClass, query), query, fields, entityClass, preparer);
    }

    @Override
    protected <T> Flux<T> doFind(String collectionName, Document query, Document fields, Class<T> entityClass, FindPublisherPreparer preparer) {
        return super.doFind(resolveCollectionNameForFindContext(collectionName, entityClass, query), query, fields, entityClass, preparer);
    }

    @Override
    protected <T> Flux<T> doFindAndDelete(String collectionName, Query query, Class<T> entityClass) {
        Flux<T> flux = find(query, entityClass, collectionName);

        return Flux.from(flux).collectList().filter(it -> !it.isEmpty())
                .flatMapMany(list -> {
                    MongoPersistentEntity<T> persistentEntity =
                            (MongoPersistentEntity<T>) this.getConverter().getMappingContext().getPersistentEntity(entityClass);

                    MultiValueMap<String, Object> byIds = new LinkedMultiValueMap<>();
                    list.forEach(resultEntry -> {
                        byIds.add(ID_KEY, persistentEntity.getPropertyAccessor(resultEntry).getProperty(
                                persistentEntity.getIdProperty()));
                    });

                    Criteria[] criterias = byIds.entrySet().stream() //
                            .map(it -> Criteria.where(it.getKey()).in(it.getValue())) //
                            .toArray(Criteria[]::new);
                    return Flux.from(remove(
                                    new Query(criterias.length == 1 ? criterias[0] : new Criteria().orOperator(criterias)), entityClass, collectionName))
                            .flatMap(deleteResult -> Flux.fromIterable(list));
                });
    }

    @Override
    protected <T> Mono<T> doFindAndRemove(String collectionName, Document query, Document fields, Document sort, Collation collation, Class<T> entityClass) {
        return super.doFindAndRemove(resolveCollectionNameForDeleteContext(collectionName, entityClass, query), query, fields, sort, collation, entityClass);
    }

    @Override
    protected Mono<UpdateResult> doUpdate(String collectionName, Query query, UpdateDefinition update, Class<?> entityClass, boolean upsert, boolean multi) {
        return super.doUpdate(resolveCollectionNameForUpdateContext(collectionName, entityClass, query, update), query, update, entityClass, upsert, multi);
    }

    @Override
    protected Mono<Long> doCount(String collectionName, Document filter, CountOptions options) {
        return super.doCount(resolveCollectionNameWithoutEntityContext(collectionName), filter, options);
    }

    @Override
    protected Mono<Long> doEstimatedCount(String collectionName, EstimatedDocumentCountOptions options) {
        return super.doEstimatedCount(resolveCollectionNameWithoutEntityContext(collectionName), options);
    }

    @Override
    public Mono<MongoCollection<Document>> getCollection(String collectionName) {
        return super.getCollection(resolveCollectionNameWithoutEntityContext(collectionName));
    }


    @Override
    public Mono<Boolean> collectionExists(String collectionName) {
        return super.collectionExists(resolveCollectionNameWithoutEntityContext(collectionName));
    }

    public Mono<Boolean> collectionExists(String collectionName, final String collectionHint) {
        return super.collectionExists(getShardingOptions().resolveCollectionName(collectionName, collectionHint));
    }

    private Mono<MongoCollection<Document>> getCollectionWithoutHintResolution(String collectionName) {
        return super.getCollection(collectionName);
    }

    @Override
    protected Mono<MongoCollection<Document>> doCreateCollection(String collectionName, CreateCollectionOptions collectionOptions) {
        String resolvedCollectionName = resolveCollectionNameWithoutEntityContext(collectionName);
        return createMono(db -> db.createCollection(resolvedCollectionName, collectionOptions)).doOnSuccess(it -> {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Created collection [{}]", collectionName);
            }
        }).then(getCollectionWithoutHintResolution(resolvedCollectionName));
    }
}
