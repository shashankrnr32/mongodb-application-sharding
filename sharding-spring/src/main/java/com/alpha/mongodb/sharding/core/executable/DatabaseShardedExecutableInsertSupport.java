package com.alpha.mongodb.sharding.core.executable;


import com.alpha.mongodb.sharding.core.assitant.DatabaseShardingAssistant;
import com.alpha.mongodb.sharding.core.callback.HintResolutionCallbacks;
import com.alpha.mongodb.sharding.core.configuration.DatabaseShardingOptions;
import com.alpha.mongodb.sharding.core.exception.UnresolvableDatabaseShardException;
import com.alpha.mongodb.sharding.core.hint.ShardingHint;
import com.alpha.mongodb.sharding.core.template.ExtendedMongoTemplate;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.lang.Nullable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.ExecutableInsertOperation;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@RequiredArgsConstructor
public class DatabaseShardedExecutableInsertSupport<T> implements
        ExecutableInsertOperation.ExecutableInsert<T>, DatabaseShardingAssistant<ExtendedMongoTemplate> {

    @Getter
    private final Map<String, ExtendedMongoTemplate> delegatedShardedMongoTemplateMap;
    private final Class<T> domainType;
    @Nullable
    private final String collection;
    @Nullable
    private final BulkOperations.BulkMode bulkMode;

    @Getter
    private final HintResolutionCallbacks hintResolutionCallbacks;
    @Getter
    private final DatabaseShardingOptions shardingOptions;

    @Override
    public T one(T object) {
        Assert.notNull(object, "Object must not be null!");
        ExtendedMongoTemplate resolvedMongoTemplate = getDelegatedTemplateForSaveContext(object);
        return resolvedMongoTemplate.insert(object, getCollectionName(resolvedMongoTemplate));
    }

    @Override
    public Collection<T> all(Collection<? extends T> objects) {
        Assert.notNull(objects, "Objects must not be null!");
        Map<String, List<T>> dividedBatch = new HashMap<>();

        for (T entity : objects) {
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
    public BulkWriteResult bulk(Collection<? extends T> objects) {
        Assert.notNull(objects, "Objects must not be null!");
        ExtendedMongoTemplate resolvedMongoTemplate = getDelegatedTemplateForSaveContext(CollectionUtils.get(objects, 0));
        return resolvedMongoTemplate.bulkOps(bulkMode != null ? bulkMode : BulkOperations.BulkMode.ORDERED, domainType, getCollectionName(resolvedMongoTemplate))
                .insert(new ArrayList<>(objects)).execute();
    }


    @Override
    public ExecutableInsertOperation.InsertWithBulkMode<T> inCollection(String collection) {
        Assert.hasText(collection, "Collection must not be null nor empty.");
        return new DatabaseShardedExecutableInsertSupport<>(delegatedShardedMongoTemplateMap, domainType, collection, bulkMode, hintResolutionCallbacks, shardingOptions);
    }


    @Override
    public ExecutableInsertOperation.TerminatingBulkInsert<T> withBulkMode(BulkOperations.BulkMode bulkMode) {
        Assert.notNull(bulkMode, "BulkMode must not be null!");
        return new DatabaseShardedExecutableInsertSupport<>(delegatedShardedMongoTemplateMap, domainType, collection, bulkMode, hintResolutionCallbacks, shardingOptions);
    }

    private String getCollectionName(ExtendedMongoTemplate template) {
        return StringUtils.hasText(collection) ? collection : template.getCollectionName(domainType);
    }
}