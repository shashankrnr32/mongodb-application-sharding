package com.alpha.mongodb.sharding.core;

import com.alpha.mongodb.sharding.core.configuration.ShardingOptions;
import com.alpha.mongodb.sharding.core.exception.UnresolvableCollectionShardException;
import com.alpha.mongodb.sharding.core.exception.UnresolvableDatabaseShardException;
import com.alpha.mongodb.sharding.core.hint.ShardingHint;
import com.alpha.mongodb.sharding.core.hint.ShardingHintManager;
import com.mongodb.client.MongoClient;
import lombok.Getter;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.lang.Nullable;

import java.util.Optional;

/**
 * Abstract Base Sharded Mongo Template
 *
 * @author Shashank Sharma
 */
public abstract class ShardedMongoTemplate extends MongoTemplate {

    @Getter
    private final ShardingOptions shardingOptions;

    public ShardedMongoTemplate(MongoClient mongoClient, String databaseName, final ShardingOptions shardingOptions) {
        super(mongoClient, databaseName);
        this.shardingOptions = shardingOptions;
    }

    public ShardedMongoTemplate(MongoDatabaseFactory mongoDbFactory, final ShardingOptions shardingOptions) {
        super(mongoDbFactory);
        this.shardingOptions = shardingOptions;
    }

    public ShardedMongoTemplate(MongoDatabaseFactory mongoDbFactory, MongoConverter mongoConverter, final ShardingOptions shardingOptions) {
        super(mongoDbFactory, mongoConverter);
        this.shardingOptions = shardingOptions;
    }

    /**
     * CCount the records from all shards that satisfies the query
     *
     * @param query       Query
     * @param entityClass Entity Class
     * @return Count of all the records satisfying the query.
     */
    public abstract long countFromAll(Query query, Class<?> entityClass);

    /**
     * Count the records from all shards that satisfies the query
     *
     * @param query          Query
     * @param collectionName Base collection Name
     * @return Count of all the records satisfying the query.
     */
    public abstract long countFromAll(Query query, String collectionName);

    /**
     * Count the records from all shards that satisfies the query
     *
     * @param query          Query
     * @param entityClass    Entity Class
     * @param collectionName Base collection Name
     * @return Count of all the records satisfying the query.
     */
    public abstract long countFromAll(Query query, @Nullable Class<?> entityClass, String collectionName);

    /**
     * Estimated count from the collection
     *
     * @param collectionName Collection Name
     * @return Base collection Name
     */
    public abstract long estimatedCountFromAllShards(String collectionName);

    protected String resolveCollectionHintWithoutEntityContext() throws UnresolvableCollectionShardException {
        Optional<ShardingHint> hint = ShardingHintManager.get();
        if (hint.isPresent() && null != hint.get().getCollectionHint()) {
            return hint.get().getCollectionHint();
        } else {
            throw new UnresolvableCollectionShardException();
        }
    }

    protected String resolveDatabaseHintWithoutEntityContext() throws UnresolvableDatabaseShardException {
        Optional<ShardingHint> hint = ShardingHintManager.get();
        if (hint.isPresent() && null != hint.get().getDatabaseHint()) {
            return hint.get().getDatabaseHint();
        } else {
            throw new UnresolvableCollectionShardException();
        }
    }

    /**
     * Resolve the collection name when there is no entity context. Here, {@link ShardingHintManager} is used
     * to determine the collection.
     *
     * @param collectionName Base Collection Name
     * @return Resolved Collection Name
     * @throws UnresolvableCollectionShardException when the flow is unable to determine the hint
     */
    protected String resolveCollectionNameWithoutEntityContext(final String collectionName)
            throws UnresolvableCollectionShardException {
        return this.shardingOptions.resolveCollectionName(collectionName, resolveCollectionHintWithoutEntityContext());
    }

    /**
     * Resolve the database name when there is no entity context. Here, {@link ShardingHintManager} is used
     * to determine the database name.
     *
     * @param databaseName Base Database Name
     * @return Resolved Collection Name
     * @throws UnresolvableCollectionShardException when the flow is unable to determine the hint
     */
    protected String resolveDatabaseNameWithoutEntityContext(final String databaseName)
            throws UnresolvableDatabaseShardException {
        return this.shardingOptions.resolveDatabaseName(databaseName, resolveDatabaseHintWithoutEntityContext());
    }
}
