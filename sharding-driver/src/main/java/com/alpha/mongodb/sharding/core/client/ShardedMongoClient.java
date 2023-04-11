package com.alpha.mongodb.sharding.core.client;

import com.alpha.mongodb.sharding.core.configuration.ShardingOptions;
import com.mongodb.client.MongoClient;

public interface ShardedMongoClient extends MongoClient {

    ShardingOptions getShardingOptions();
}
