package com.alpha.mongodb.sharding.core.entity;

public interface CollectionShardedEntity extends ShardedEntity {
    String getCollectionHint();
}
