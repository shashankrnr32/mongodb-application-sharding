package com.alpha.mongodb.sharding.core;

import com.alpha.mongodb.sharding.core.configuration.CompositeShardingOptions;
import com.mongodb.client.MongoClient;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Composite Sharded Mongo Client
 *
 * @author Shashank Sharma
 */
public class CompositeShardedMongoClient extends DatabaseShardedMongoClient {

    public CompositeShardedMongoClient(Map<String, MongoClient> delegatedMongoClientMap, CompositeShardingOptions shardingOptions) {
        super(delegatedMongoClientMap.entrySet().stream().collect(
                Collectors.toMap(Map.Entry::getKey, v -> new CollectionShardedMongoClient(
                        v.getValue(), shardingOptions.getDelegatedCollectionShardingOptions()))), shardingOptions);
    }

    public CompositeShardedMongoClient(MongoClient delegatedMongoClient, CompositeShardingOptions shardingOptions) {
        this(shardingOptions.getDefaultDatabaseHintsSet().stream().collect(Collectors.toMap(h -> h, h -> delegatedMongoClient)), shardingOptions);
    }
}
