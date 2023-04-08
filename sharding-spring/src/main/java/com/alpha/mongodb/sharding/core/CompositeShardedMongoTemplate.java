package com.alpha.mongodb.sharding.core;

import com.alpha.mongodb.sharding.core.configuration.CompositeShardingOptions;
import com.mongodb.client.MongoClient;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.query.Query;

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

    @Override
    public long countFromAll(Query query, Class<?> entityClass) {
        return this.getDelegatedShardedMongoTemplateMap().values().stream().mapToLong(mongoTemplate -> ((CollectionShardedMongoTemplate) mongoTemplate).countFromAll(query, entityClass)).sum();
    }

    @Override
    public long countFromAll(Query query, String collectionName) {
        return this.getDelegatedShardedMongoTemplateMap().values().stream().mapToLong(mongoTemplate -> ((CollectionShardedMongoTemplate) mongoTemplate).countFromAll(query, collectionName)).sum();
    }

    @Override
    public long countFromAll(Query query, Class<?> entityClass, String collectionName) {
        return this.getDelegatedShardedMongoTemplateMap().values().stream().mapToLong(mongoTemplate -> ((CollectionShardedMongoTemplate) mongoTemplate).countFromAll(query, entityClass, collectionName)).sum();
    }
}
