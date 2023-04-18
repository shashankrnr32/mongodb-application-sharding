package com.alpha.mongodb.sharding.core.executable;

import com.alpha.mongodb.sharding.core.assitant.DatabaseShardingAssistant;
import com.alpha.mongodb.sharding.core.callback.HintResolutionCallbacks;
import com.alpha.mongodb.sharding.core.configuration.DatabaseShardingOptions;
import com.alpha.mongodb.sharding.core.template.ExtendedMongoTemplate;
import com.mongodb.ReadPreference;
import com.mongodb.client.FindIterable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.bson.Document;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.mongodb.core.CursorPreparer;
import org.springframework.data.mongodb.core.ExecutableFindOperation;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.SerializationUtils;
import org.springframework.data.util.CloseableIterator;
import org.springframework.data.util.StreamUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Implementation of {@link ExecutableFindOperation} modified for
 * Database Sharding
 *
 * @author Shashank Sharma
 */
@RequiredArgsConstructor
public class DatabaseShardedExecutableFindSupport<T>
        implements ExecutableFindOperation.ExecutableFind<T>, ExecutableFindOperation.FindWithCollection<T>, ExecutableFindOperation.FindWithProjection<T>, ExecutableFindOperation.FindWithQuery<T>,
        DatabaseShardingAssistant<ExtendedMongoTemplate> {

    @Getter
    private final Map<String, ExtendedMongoTemplate> delegatedShardedMongoTemplateMap;

    private final Class<?> domainType;
    private final Class<T> returnType;
    @Nullable
    private final String collection;
    private final Query query;

    @Getter
    private final HintResolutionCallbacks hintResolutionCallbacks;
    @Getter
    private final DatabaseShardingOptions shardingOptions;

    @Override
    public ExecutableFindOperation.FindWithProjection<T> inCollection(String collection) {
        Assert.hasText(collection, "Collection name must not be null nor empty!");
        return new DatabaseShardedExecutableFindSupport<>(delegatedShardedMongoTemplateMap, domainType, returnType, collection, query, hintResolutionCallbacks, shardingOptions);
    }

    @Override
    public <T1> ExecutableFindOperation.FindWithQuery<T1> as(Class<T1> returnType) {
        Assert.notNull(returnType, "ReturnType must not be null!");
        return new DatabaseShardedExecutableFindSupport<>(
                delegatedShardedMongoTemplateMap, domainType, returnType, collection, query, hintResolutionCallbacks, shardingOptions);
    }

    @Override
    public ExecutableFindOperation.TerminatingFind<T> matching(Query query) {
        Assert.notNull(query, "Query must not be null!");
        return new DatabaseShardedExecutableFindSupport<>(
                delegatedShardedMongoTemplateMap, domainType, returnType, collection, query, hintResolutionCallbacks, shardingOptions);
    }

    @Override
    public T oneValue() {

        List<T> result = doFind(null);

        if (ObjectUtils.isEmpty(result)) {
            return null;
        }

        if (result.size() > 1) {
            throw new IncorrectResultSizeDataAccessException("Query " + asString() + " returned non unique result.", 1);
        }

        return result.iterator().next();
    }

    @Override
    public T firstValue() {

        List<T> result = doFind(null);
        return ObjectUtils.isEmpty(result) ? null : result.iterator().next();
    }

    @Override
    public List<T> all() {
        return doFind(null);
    }

    @Override
    public Stream<T> stream() {
        return StreamUtils.createStreamFromIterator(doStream());
    }

    @Override
    public ExecutableFindOperation.TerminatingFindNear<T> near(NearQuery nearQuery) {
        return () -> {
            ExtendedMongoTemplate resolvedMongoTemplate = getDelegatedTemplateForFindContext(domainType, query);
            return resolvedMongoTemplate.geoNear(nearQuery, domainType, getCollectionName(resolvedMongoTemplate), returnType);
        };
    }

    @Override
    public long count() {
        ExtendedMongoTemplate resolvedMongoTemplate = getDelegatedTemplateForFindContext(domainType, query);
        return resolvedMongoTemplate.count(query, domainType, getCollectionName(resolvedMongoTemplate));
    }

    @Override
    public boolean exists() {
        ExtendedMongoTemplate resolvedMongoTemplate = getDelegatedTemplateForFindContext(domainType, query);
        return resolvedMongoTemplate.exists(query, domainType, getCollectionName(resolvedMongoTemplate));
    }

    @SuppressWarnings("unchecked")
    @Override
    public ExecutableFindOperation.TerminatingDistinct<Object> distinct(String field) {
        Assert.notNull(field, "Field must not be null!");
        return new DistinctOperationSupport(this, field);
    }

    private List<T> doFind(@Nullable CursorPreparer preparer) {
        ExtendedMongoTemplate resolvedMongoTemplate = getDelegatedTemplateForFindContext(domainType, query);
        return resolvedMongoTemplate.find(query, returnType, getCollectionName(resolvedMongoTemplate));
    }

    private List<T> doFindDistinct(String field) {
        ExtendedMongoTemplate resolvedMongoTemplate = getDelegatedTemplateForFindContext(domainType, query);
        return resolvedMongoTemplate.findDistinct(query, field, getCollectionName(resolvedMongoTemplate), domainType,
                returnType == domainType ? (Class<T>) Object.class : returnType);
    }

    private CloseableIterator<T> doStream() {
        ExtendedMongoTemplate resolvedMongoTemplate = getDelegatedTemplateForFindContext(domainType, query);
        return resolvedMongoTemplate.stream(query, returnType, getCollectionName(resolvedMongoTemplate));
    }

    private String getCollectionName(ExtendedMongoTemplate extendedMongoTemplate) {
        return StringUtils.hasText(collection) ? collection : extendedMongoTemplate.getCollectionName(domainType);
    }

    private String asString() {
        return SerializationUtils.serializeToJsonSafely(query);
    }


    static class DelegatingQueryCursorPreparer implements CursorPreparer {

        private final @Nullable
        CursorPreparer delegate;
        private Optional<Integer> limit = Optional.empty();

        DelegatingQueryCursorPreparer(@Nullable CursorPreparer delegate) {
            this.delegate = delegate;
        }

        @Override
        public FindIterable<Document> prepare(FindIterable<Document> iterable) {
            FindIterable<Document> target = delegate != null ? delegate.prepare(iterable) : iterable;
            return limit.map(target::limit).orElse(target);
        }

        CursorPreparer limit(int limit) {
            this.limit = Optional.of(limit);
            return this;
        }

        @Override
        public ReadPreference getReadPreference() {
            return delegate.getReadPreference();
        }
    }

    static class DistinctOperationSupport<T> implements ExecutableFindOperation.TerminatingDistinct<T> {

        private final String field;
        private final DatabaseShardedExecutableFindSupport<T> delegate;

        public DistinctOperationSupport(DatabaseShardedExecutableFindSupport<T> delegate, String field) {

            this.delegate = delegate;
            this.field = field;
        }

        /*
         * (non-Javadoc)
         * @see org.springframework.data.mongodb.core.ExecutableFindOperation.DistinctWithProjection#as(java.lang.Class)
         */
        @Override
        public <R> ExecutableFindOperation.TerminatingDistinct<R> as(Class<R> resultType) {

            Assert.notNull(resultType, "ResultType must not be null!");

            return new DistinctOperationSupport<>((DatabaseShardedExecutableFindSupport<R>) delegate.as(resultType), field);
        }

        @Override
        public ExecutableFindOperation.TerminatingDistinct<T> matching(Query query) {

            Assert.notNull(query, "Query must not be null!");

            return new DistinctOperationSupport<>((DatabaseShardedExecutableFindSupport<T>) delegate.matching(query), field);
        }

        @Override
        public List<T> all() {
            return delegate.doFindDistinct(field);
        }
    }
}