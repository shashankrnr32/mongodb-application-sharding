package com.alpha.mongodb.sharding.core.entity;

public interface MongoDBCollectionShardedEntity extends MongoDBShardedEntity {
    String getCollectionHint();
}
