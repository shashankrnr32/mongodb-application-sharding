package com.alpha.mongodb.sharding.core.client;

import com.alpha.mongodb.sharding.core.configuration.CompositeShardingOptions;
import com.mongodb.reactivestreams.client.MongoClient;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Composite Sharded Reactive Mongo Client
 *
 * @author Shashank Sharma
 */
public class CompositeShardedReactiveMongoClient extends DatabaseShardedReactiveMongoClient {

    public CompositeShardedReactiveMongoClient(Map<String, MongoClient> delegatedMongoClientMap, CompositeShardingOptions shardingOptions) {
        super(delegatedMongoClientMap.entrySet().stream().collect(
                Collectors.toMap(Map.Entry::getKey, v -> new CollectionShardedReactiveMongoClient(
                        v.getValue(), shardingOptions.getDelegatedCollectionShardingOptions()))), shardingOptions);
    }

    public CompositeShardedReactiveMongoClient(MongoClient delegatedMongoClient, CompositeShardingOptions shardingOptions) {
        this(shardingOptions.getDefaultDatabaseHintsSet().stream().collect(Collectors.toMap(h -> h, h -> delegatedMongoClient)), shardingOptions);
    }
}
