package com.alpha.mongodb.sharding.core.template;

import com.alpha.mongodb.sharding.core.configuration.CompositeShardingOptions;
import com.mongodb.client.MongoClient;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.MongoConverter;

import java.util.Map;

/**
 * Composite Sharded Mongo Template to be used with collections that are
 * sharded both by Database and Collection
 *
 * @author Shashank Sharma
 */
public class CompositeShardedMongoTemplate extends DatabaseShardedMongoTemplate {

    public CompositeShardedMongoTemplate(MongoClient mongoClient, String databaseName, CompositeShardingOptions shardingOptions) {
        super(mongoClient, databaseName, shardingOptions);
    }

    public CompositeShardedMongoTemplate(Map<String, MongoDatabaseFactory> delegatedDatabaseFactory, CompositeShardingOptions shardingOptions) {
        super(delegatedDatabaseFactory, shardingOptions);
    }

    public CompositeShardedMongoTemplate(Map<String, MongoDatabaseFactory> delegatedDatabaseFactory, MongoConverter mongoConverter, CompositeShardingOptions shardingOptions) {
        super(delegatedDatabaseFactory, mongoConverter, shardingOptions);
    }
}
