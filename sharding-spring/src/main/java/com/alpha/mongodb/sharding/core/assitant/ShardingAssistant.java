package com.alpha.mongodb.sharding.core.assitant;

import com.alpha.mongodb.sharding.core.callback.HintResolutionCallbacks;
import com.alpha.mongodb.sharding.core.configuration.ShardingOptions;
import com.alpha.mongodb.sharding.core.exception.UnresolvableCollectionShardException;
import com.alpha.mongodb.sharding.core.exception.UnresolvableDatabaseShardException;
import com.alpha.mongodb.sharding.core.hint.ShardingHint;
import com.alpha.mongodb.sharding.core.hint.ShardingHintManager;

import java.util.Optional;

public interface ShardingAssistant {

    ShardingOptions getShardingOptions();

    HintResolutionCallbacks getHintResolutionCallbacks();

    default String resolveCollectionHintWithoutEntityContext() throws UnresolvableCollectionShardException {
        Optional<ShardingHint> hint = ShardingHintManager.getHint();
        if (hint.isPresent() && null != hint.get().getCollectionHint()) {
            return hint.get().getCollectionHint();
        } else {
            throw new UnresolvableCollectionShardException();
        }
    }

    default String resolveDatabaseHintWithoutEntityContext() throws UnresolvableDatabaseShardException {
        Optional<ShardingHint> hint = ShardingHintManager.getHint();
        if (hint.isPresent() && null != hint.get().getDatabaseHint()) {
            return hint.get().getDatabaseHint();
        } else {
            throw new UnresolvableDatabaseShardException();
        }
    }

    /**
     * Resolve the collection name when there is no entity context. Here, {@link ShardingHintManager} is used
     * to determine the collection.
     *
     * @param collectionName Base Collection Name
     * @return Resolved Collection Name
     * @throws UnresolvableCollectionShardException when the flow is unable to determine the hint
     */
    default String resolveCollectionNameWithoutEntityContext(final String collectionName)
            throws UnresolvableCollectionShardException {
        return getShardingOptions().resolveCollectionName(collectionName, resolveCollectionHintWithoutEntityContext());
    }

    /**
     * Resolve the database name when there is no entity context. Here, {@link ShardingHintManager} is used
     * to determine the database name.
     *
     * @param databaseName Base Database Name
     * @return Resolved Collection Name
     * @throws UnresolvableCollectionShardException when the flow is unable to determine the hint
     */
    default String resolveDatabaseNameWithoutEntityContext(final String databaseName)
            throws UnresolvableDatabaseShardException {
        return getShardingOptions().resolveDatabaseName(databaseName, resolveDatabaseHintWithoutEntityContext());
    }
}
