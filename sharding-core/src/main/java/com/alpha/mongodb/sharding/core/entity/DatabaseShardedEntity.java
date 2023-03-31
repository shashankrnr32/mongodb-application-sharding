package com.alpha.mongodb.sharding.core.entity;

public interface DatabaseShardedEntity extends ShardedEntity {
    String getDatabaseHint();
}
