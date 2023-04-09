package com.alpha.mongodb.sharding.core;

import com.alpha.mongodb.sharding.core.assitant.ShardingAssistant;
import com.alpha.mongodb.sharding.core.callback.HintResolutionCallback;
import com.alpha.mongodb.sharding.core.callback.HintResolutionCallbacks;
import com.alpha.mongodb.sharding.core.configuration.ShardingOptions;
import com.mongodb.client.MongoClient;
import lombok.Getter;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.lang.Nullable;

import java.util.Set;

/**
 * Abstract Base Sharded Mongo Template
 *
 * @author Shashank Sharma
 */
public abstract class ShardedMongoTemplate extends MongoTemplate implements ShardingAssistant {

    @Getter
    private final ShardingOptions shardingOptions;
    @Getter
    private final HintResolutionCallbacks hintResolutionCallbacks;

    protected ShardedMongoTemplate(MongoClient mongoClient, String databaseName, final ShardingOptions shardingOptions) {
        super(mongoClient, databaseName);
        this.shardingOptions = shardingOptions;
        hintResolutionCallbacks = new HintResolutionCallbacks(
                (Set<HintResolutionCallback<?>>) shardingOptions.getHintResolutionCallbacks());
    }

    protected ShardedMongoTemplate(MongoDatabaseFactory mongoDbFactory, final ShardingOptions shardingOptions) {
        super(mongoDbFactory);
        this.shardingOptions = shardingOptions;
        hintResolutionCallbacks = new HintResolutionCallbacks(
                (Set<HintResolutionCallback<?>>) shardingOptions.getHintResolutionCallbacks());
    }

    ShardedMongoTemplate(MongoDatabaseFactory mongoDbFactory, MongoConverter mongoConverter, final ShardingOptions shardingOptions) {
        super(mongoDbFactory, mongoConverter);
        this.shardingOptions = shardingOptions;
        hintResolutionCallbacks = new HintResolutionCallbacks(
                (Set<HintResolutionCallback<?>>) shardingOptions.getHintResolutionCallbacks());
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        super.setApplicationContext(applicationContext);
        hintResolutionCallbacks.discover(applicationContext);
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
}
