package com.alpha.mongodb.sharding.core.callback;

import com.alpha.mongodb.sharding.core.entity.CompositeShardedEntity;
import com.alpha.mongodb.sharding.core.hint.ShardingHint;

public interface CompositeShardedEntityHintResolutionCallback<T extends CompositeShardedEntity> extends HintResolutionCallback<T> {

    default ShardingHint resolveHintForSaveContext(T entity) {
        ShardingHint shardingHint = new ShardingHint();
        shardingHint.setDatabaseHint(entity.resolveDatabaseHint());
        shardingHint.setCollectionHint(entity.resolveCollectionHint());
        return shardingHint;
    }
}
