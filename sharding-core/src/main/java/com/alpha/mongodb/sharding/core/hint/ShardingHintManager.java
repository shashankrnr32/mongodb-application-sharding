package com.alpha.mongodb.sharding.core.hint;

import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;

/**
 * Sharding Hint to set Database and Table shard suffix
 * for the current thread. The values set for a particular query will be
 * propagated to all the queries unless explicitly cleared.
 *
 * @author Shashank Sharma
 */
@UtilityClass
public class ShardingHintManager {

    private static final ThreadLocal<ShardingHint> hintThreadLocal = new ThreadLocal<>();

    /**
     * Sets the Database Hint for the current thread.
     *
     * @param databaseHint Database Hint
     */
    public static void setDatabaseHint(final String databaseHint) {
        if (ObjectUtils.isEmpty(hintThreadLocal.get())) {
            ShardingHint shardingHint = new ShardingHint();
            shardingHint.setDatabaseHint(databaseHint);
            hintThreadLocal.set(shardingHint);
        } else {
            ShardingHint shardingHint = hintThreadLocal.get();
            if (StringUtils.isBlank(shardingHint.getDatabaseHint())) {
                shardingHint.setDatabaseHint(databaseHint);
                hintThreadLocal.set(shardingHint);
            } else {
                throw new IllegalStateException("Database hint is already set. Cannot set a new value");
            }
        }
    }

    /**
     * Sets the Collection Hint for the current thread.
     *
     * @param collectionHint Collection Hint
     */
    public static void setCollectionHint(final String collectionHint) {
        if (ObjectUtils.isEmpty(hintThreadLocal.get())) {
            ShardingHint shardingHint = new ShardingHint();
            shardingHint.setCollectionHint(collectionHint);
            hintThreadLocal.set(shardingHint);
        } else {
            ShardingHint shardingHint = hintThreadLocal.get();
            if (StringUtils.isBlank(shardingHint.getCollectionHint())) {
                shardingHint.setCollectionHint(collectionHint);
                hintThreadLocal.set(shardingHint);
            } else {
                throw new IllegalStateException("Collection hint is already set. Cannot set a new value");
            }
        }
    }

    /**
     * Set the composite hint for the current thread
     *
     * @param databaseHint   database hint
     * @param collectionHint collection hint
     */
    public static void setCompositeHint(final String databaseHint, final String collectionHint) {
        if (ObjectUtils.isEmpty(hintThreadLocal.get())) {
            ShardingHint shardingHint = new ShardingHint();
            shardingHint.setDatabaseHint(databaseHint);
            shardingHint.setCollectionHint(collectionHint);
            hintThreadLocal.set(shardingHint);
        } else {
            throw new IllegalStateException("Composite hint is already set. Cannot set a new value");
        }
    }

    /**
     * Get the current Hint value if exists
     *
     * @return Optional Hint value
     */
    public static Optional<ShardingHint> getHint() {
        return ObjectUtils.isEmpty(hintThreadLocal.get()) ?
                Optional.empty() : Optional.of(new ShardingHint(hintThreadLocal.get()));
    }

    /**
     * Clear the current value if exists and return the value
     * before clearing
     *
     * @return Optional Hint value
     */
    public static Optional<ShardingHint> clear() {
        ShardingHint shardingHint = hintThreadLocal.get();
        hintThreadLocal.remove();
        return Optional.ofNullable(shardingHint);
    }


}
