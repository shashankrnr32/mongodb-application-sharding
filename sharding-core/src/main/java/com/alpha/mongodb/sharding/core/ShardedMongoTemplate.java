package com.alpha.mongodb.sharding.core;

import com.alpha.mongodb.sharding.core.exception.UnresolvableCollectionShardException;
import com.alpha.mongodb.sharding.core.hint.ShardingHint;
import com.alpha.mongodb.sharding.core.hint.ShardingHintManager;
import com.mongodb.client.MongoClient;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import java.util.Optional;

public abstract class ShardedMongoTemplate extends MongoTemplate {

    @Getter
    @Setter
    private String shardSeparator = "_";

    public ShardedMongoTemplate(MongoClient mongoClient, String databaseName) {
        super(mongoClient, databaseName);
    }

    public ShardedMongoTemplate(MongoDatabaseFactory mongoDbFactory) {
        super(mongoDbFactory);
    }

    public ShardedMongoTemplate(MongoDatabaseFactory mongoDbFactory, MongoConverter mongoConverter) {
        super(mongoDbFactory, mongoConverter);
    }

    protected String resolveName(@NonNull final String name, @NonNull final String hint) {
        return String.format("%s%s%s", name, getShardSeparator(), hint);
    }

    /**
     * CCount the records from all shards that satisfies the query
     *
     * @param query       Query
     * @param entityClass Entity Class
     * @return Count of all the records satisfying the query.
     */
    public abstract long countFromAllShards(Query query, Class<?> entityClass);

    /**
     * Count the records from all shards that satisfies the query
     *
     * @param query          Query
     * @param collectionName Base collection Name
     * @return Count of all the records satisfying the query.
     */
    public abstract long countFromAllShards(Query query, String collectionName);

    /**
     * Count the records from all shards that satisfies the query
     *
     * @param query          Query
     * @param entityClass    Entity Class
     * @param collectionName Base collection Name
     * @return Count of all the records satisfying the query.
     */
    public abstract long countFromAllShards(Query query, @Nullable Class<?> entityClass, String collectionName);

    /**
     * Estimated count from the collection
     *
     * @param collectionName Collection Name
     * @return Base collection Name
     */
    public abstract long estimatedCountFromAllShards(String collectionName);

    /**
     * Resolve the collection name when there is no entity context. Here, {@link ShardingHintManager} is used
     * to determine the collection.
     *
     * @param collectionName Base Collection Name
     * @return Resolved Collection Name
     */
    protected String resolveCollectionNameWithoutEntityContext(final String collectionName) {
        String resolvedCollectionName;
        Optional<ShardingHint> hint = ShardingHintManager.get();
        if (hint.isPresent() && null != hint.get().getCollectionHint()) {
            resolvedCollectionName = resolveName(collectionName, hint.get().getCollectionHint());
        } else {
            throw new UnresolvableCollectionShardException();
        }
        return resolvedCollectionName;
    }
}
