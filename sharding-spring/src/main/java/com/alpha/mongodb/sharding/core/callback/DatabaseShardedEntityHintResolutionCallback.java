package com.alpha.mongodb.sharding.core.callback;

import com.alpha.mongodb.sharding.core.entity.DatabaseShardedEntity;
import com.alpha.mongodb.sharding.core.hint.ShardingHint;

public interface DatabaseShardedEntityHintResolutionCallback<T extends DatabaseShardedEntity> extends HintResolutionCallback<T> {

    default ShardingHint resolveHintForSaveContext(T entity) {
        ShardingHint shardingHint = new ShardingHint();
        shardingHint.setDatabaseHint(entity.resolveDatabaseHint());
        return shardingHint;
    }
}
