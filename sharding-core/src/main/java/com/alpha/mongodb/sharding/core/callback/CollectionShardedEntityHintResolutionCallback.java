package com.alpha.mongodb.sharding.core.callback;

import com.alpha.mongodb.sharding.core.entity.CollectionShardedEntity;
import com.alpha.mongodb.sharding.core.hint.ShardingHint;

public interface CollectionShardedEntityHintResolutionCallback<T extends CollectionShardedEntity> extends HintResolutionCallback<T> {

    default ShardingHint resolveHintForSaveContext(T entity) {
        ShardingHint shardingHint = new ShardingHint();
        shardingHint.setCollectionHint(entity.resolveCollectionHint());
        return shardingHint;
    }
}
