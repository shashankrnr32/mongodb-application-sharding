package com.alpha.mongodb.sharding.core.executable;

import com.alpha.mongodb.sharding.core.assitant.DatabaseShardingAssistant;
import com.alpha.mongodb.sharding.core.callback.HintResolutionCallbacks;
import com.alpha.mongodb.sharding.core.configuration.DatabaseShardingOptions;
import com.alpha.mongodb.sharding.core.template.ExtendedMongoTemplate;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.lang.Nullable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.data.mongodb.core.ExecutableUpdateOperation;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.FindAndReplaceOptions;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.Map;

@RequiredArgsConstructor
public class DatabaseShardedExecutableUpdateSupport<T>
        implements ExecutableUpdateOperation.ExecutableUpdate<T>, ExecutableUpdateOperation.UpdateWithCollection<T>, ExecutableUpdateOperation.UpdateWithQuery<T>, ExecutableUpdateOperation.TerminatingUpdate<T>,
        ExecutableUpdateOperation.FindAndReplaceWithOptions<T>, ExecutableUpdateOperation.TerminatingFindAndReplace<T>, ExecutableUpdateOperation.FindAndReplaceWithProjection<T>, DatabaseShardingAssistant<ExtendedMongoTemplate> {

    @Getter
    private final Map<String, ExtendedMongoTemplate> delegatedShardedMongoTemplateMap;
    private final Class domainType;
    private final Query query;
    @Nullable
    private final UpdateDefinition update;
    @Nullable
    private final String collection;
    @Nullable
    private final FindAndModifyOptions findAndModifyOptions;
    @Nullable
    private final FindAndReplaceOptions findAndReplaceOptions;
    @Nullable
    private final Object replacement;
    private final Class<T> targetType;

    @Getter
    private final HintResolutionCallbacks hintResolutionCallbacks;

    @Getter
    private final DatabaseShardingOptions shardingOptions;

    /*
     * (non-Javadoc)
     * @see org.springframework.data.mongodb.core.ExecutableUpdateOperation.UpdateWithUpdate#apply(org.springframework.data.mongodb.core.query.UpdateDefinition)
     */
    @Override
    public ExecutableUpdateOperation.TerminatingUpdate<T> apply(UpdateDefinition update) {

        Assert.notNull(update, "Update must not be null!");

        return new DatabaseShardedExecutableUpdateSupport(delegatedShardedMongoTemplateMap, domainType, query, update, collection, findAndModifyOptions,
                findAndReplaceOptions, replacement, targetType, hintResolutionCallbacks, shardingOptions);
    }

    @Override
    public ExecutableUpdateOperation.UpdateWithQuery<T> inCollection(String collection) {
        Assert.hasText(collection, "Collection must not be null nor empty!");
        return new DatabaseShardedExecutableUpdateSupport<>(getDelegatedShardedMongoTemplateMap(), domainType, query, update, collection, findAndModifyOptions,
                findAndReplaceOptions, replacement, targetType, hintResolutionCallbacks, shardingOptions);
    }

    @Override
    public ExecutableUpdateOperation.TerminatingFindAndModify<T> withOptions(FindAndModifyOptions options) {
        Assert.notNull(options, "Options must not be null!");
        return new DatabaseShardedExecutableUpdateSupport<>(delegatedShardedMongoTemplateMap, domainType, query, update, collection, options,
                findAndReplaceOptions, replacement, targetType, hintResolutionCallbacks, shardingOptions);
    }

    @Override
    public ExecutableUpdateOperation.FindAndReplaceWithProjection<T> replaceWith(T replacement) {
        Assert.notNull(replacement, "Replacement must not be null!");
        return new DatabaseShardedExecutableUpdateSupport<>(delegatedShardedMongoTemplateMap, domainType, query, update, collection, findAndModifyOptions,
                findAndReplaceOptions, replacement, targetType, hintResolutionCallbacks, shardingOptions);
    }

    @Override
    public ExecutableUpdateOperation.FindAndReplaceWithProjection<T> withOptions(FindAndReplaceOptions options) {
        Assert.notNull(options, "Options must not be null!");
        return new DatabaseShardedExecutableUpdateSupport<>(delegatedShardedMongoTemplateMap, domainType, query, update, collection, findAndModifyOptions,
                options, replacement, targetType, hintResolutionCallbacks, shardingOptions);
    }

    @Override
    public ExecutableUpdateOperation.UpdateWithUpdate<T> matching(Query query) {
        Assert.notNull(query, "Query must not be null!");
        return new DatabaseShardedExecutableUpdateSupport<>(delegatedShardedMongoTemplateMap, domainType, query, update, collection, findAndModifyOptions,
                findAndReplaceOptions, replacement, targetType, hintResolutionCallbacks, shardingOptions);
    }

    @Override
    public <R> ExecutableUpdateOperation.FindAndReplaceWithOptions<R> as(Class<R> resultType) {
        Assert.notNull(resultType, "ResultType must not be null!");
        return new DatabaseShardedExecutableUpdateSupport<>(delegatedShardedMongoTemplateMap, domainType, query, update, collection, findAndModifyOptions,
                findAndReplaceOptions, replacement, resultType, hintResolutionCallbacks, shardingOptions);
    }

    @Override
    public UpdateResult all() {
        return doUpdate(true, false);
    }

    @Override
    public UpdateResult first() {
        return doUpdate(false, false);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.mongodb.core.ExecutableUpdateOperation.TerminatingUpdate#upsert()
     */
    @Override
    public UpdateResult upsert() {
        return doUpdate(true, true);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.mongodb.core.ExecutableUpdateOperation.TerminatingFindAndModify#findAndModifyValue()
     */
    @Override
    public @Nullable
    T findAndModifyValue() {
        ExtendedMongoTemplate extendedMongoTemplate = getDelegatedTemplateForUpdateContext(targetType, query, update);
        return extendedMongoTemplate.findAndModify(query, update,
                findAndModifyOptions != null ? findAndModifyOptions : new FindAndModifyOptions(), targetType,
                getCollectionName(extendedMongoTemplate));
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.mongodb.core.ExecutableUpdateOperation.TerminatingFindAndReplace#findAndReplaceValue()
     */
    @Override
    public @Nullable
    T findAndReplaceValue() {
        ExtendedMongoTemplate extendedMongoTemplate = getDelegatedTemplateForUpdateContext(targetType, query, update);
        return (T) extendedMongoTemplate.findAndReplace(query, replacement,
                findAndReplaceOptions != null ? findAndReplaceOptions : FindAndReplaceOptions.empty(), domainType,
                getCollectionName(extendedMongoTemplate), targetType);
    }

    private UpdateResult doUpdate(boolean multi, boolean upsert) {
        ExtendedMongoTemplate resolvedMongoTemplate = getDelegatedTemplateForUpdateContext(targetType, query, update);
        String collectionName = getCollectionName(resolvedMongoTemplate);
        if (upsert) {
            return resolvedMongoTemplate.upsert(query, update, targetType, collectionName);
        } else {
            if (multi) {
                return resolvedMongoTemplate.updateMulti(query, update, targetType, collectionName);
            } else {
                return resolvedMongoTemplate.updateFirst(query, update, targetType, collectionName);
            }
        }
    }

    private String getCollectionName(ExtendedMongoTemplate extendedMongoTemplate) {
        return StringUtils.hasText(collection) ? collection : extendedMongoTemplate.getCollectionName(domainType);
    }
}