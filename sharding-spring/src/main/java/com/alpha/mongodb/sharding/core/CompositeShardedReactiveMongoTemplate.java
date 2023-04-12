package com.alpha.mongodb.sharding.core;

import com.alpha.mongodb.sharding.core.configuration.CompositeShardingOptions;
import com.mongodb.reactivestreams.client.MongoClient;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.MongoConverter;

import java.util.Map;

/**
 * Composite Sharded Reactive Mongo Template to be used with collections that are
 * sharded both by Database and Collection
 *
 * @author Shashank Sharma
 */
public class CompositeShardedReactiveMongoTemplate extends DatabaseShardedReactiveMongoTemplate {

    public CompositeShardedReactiveMongoTemplate(MongoClient mongoClient, String databaseName, CompositeShardingOptions shardingOptions) {
        super(mongoClient, databaseName, shardingOptions);
    }

    public CompositeShardedReactiveMongoTemplate(Map<String, ReactiveMongoDatabaseFactory> delegatedDatabaseFactory, CompositeShardingOptions shardingOptions) {
        super(delegatedDatabaseFactory, shardingOptions);
    }

    public CompositeShardedReactiveMongoTemplate(Map<String, ReactiveMongoDatabaseFactory> delegatedDatabaseFactory, MongoConverter mongoConverter, CompositeShardingOptions shardingOptions) {
        super(delegatedDatabaseFactory, mongoConverter, shardingOptions);
    }
}
