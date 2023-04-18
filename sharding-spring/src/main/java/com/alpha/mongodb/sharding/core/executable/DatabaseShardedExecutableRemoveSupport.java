package com.alpha.mongodb.sharding.core.executable;

import com.alpha.mongodb.sharding.core.assitant.DatabaseShardingAssistant;
import com.alpha.mongodb.sharding.core.callback.HintResolutionCallbacks;
import com.alpha.mongodb.sharding.core.configuration.DatabaseShardingOptions;
import com.alpha.mongodb.sharding.core.template.ExtendedMongoTemplate;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.lang.Nullable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.data.mongodb.core.ExecutableRemoveOperation;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
public class DatabaseShardedExecutableRemoveSupport<T>
        implements ExecutableRemoveOperation.ExecutableRemove<T>, ExecutableRemoveOperation.RemoveWithCollection<T>,
        DatabaseShardingAssistant<ExtendedMongoTemplate> {

    @Getter
    private final Map<String, ExtendedMongoTemplate> delegatedShardedMongoTemplateMap;
    private final Class<T> domainType;
    @Nullable
    private final String collection;
    private final Query query;
    @Getter
    private final HintResolutionCallbacks hintResolutionCallbacks;
    @Getter
    private final DatabaseShardingOptions shardingOptions;

    @Override
    public ExecutableRemoveOperation.RemoveWithQuery<T> inCollection(String collection) {
        Assert.hasText(collection, "Collection must not be null nor empty!");
        return new DatabaseShardedExecutableRemoveSupport<>(
                delegatedShardedMongoTemplateMap, domainType, collection, query, hintResolutionCallbacks, shardingOptions);
    }

    @Override
    public ExecutableRemoveOperation.TerminatingRemove<T> matching(Query query) {
        Assert.notNull(query, "Query must not be null!");
        return new DatabaseShardedExecutableRemoveSupport<>(
                delegatedShardedMongoTemplateMap, domainType, collection, query, hintResolutionCallbacks, shardingOptions);
    }

    @Override
    public DeleteResult all() {
        ExtendedMongoTemplate resolvedMongoTemplate = getDelegatedTemplateForDeleteContext(domainType, query);
        return resolvedMongoTemplate.remove(query, domainType, getCollectionName(resolvedMongoTemplate));
    }


    @Override
    public DeleteResult one() {
        ExtendedMongoTemplate resolvedMongoTemplate = getDelegatedTemplateForDeleteContext(domainType, query);
        return resolvedMongoTemplate.removeOne(query, domainType, getCollectionName(resolvedMongoTemplate));
    }

    @Override
    public List<T> findAndRemove() {
        ExtendedMongoTemplate resolvedMongoTemplate = getDelegatedTemplateForFindContext(domainType, query);
        return resolvedMongoTemplate.findAllAndRemove(query, domainType, getCollectionName(resolvedMongoTemplate));
    }

    private String getCollectionName(ExtendedMongoTemplate extendedMongoTemplate) {
        return StringUtils.hasText(collection) ? collection : extendedMongoTemplate.getCollectionName(domainType);
    }
}