package com.alpha.mongodb.sharding.core.entity;

import java.lang.annotation.Documented;

public interface MongoDBDatabaseShardedEntity extends MongoDBShardedEntity {
    String getDatabaseShardSuffix();
}
