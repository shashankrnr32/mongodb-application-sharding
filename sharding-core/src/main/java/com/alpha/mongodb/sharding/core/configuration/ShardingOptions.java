package com.alpha.mongodb.sharding.core.configuration;

import com.alpha.mongodb.sharding.core.callback.HintResolutionCallback;
import lombok.Data;

import java.util.HashSet;
import java.util.Set;

/**
 * Base class for Sharding options.
 *
 * @author Shashank Sharma
 */
@Data
public class ShardingOptions {
    /**
     * The Shard Separator String that is inserted in between the collection/database
     * name and the hint
     */
    private String shardSeparator = "_";

    private Set<HintResolutionCallback<?>> hintResolutionCallbacks = new HashSet<>();

    /**
     * Provides default implementation to resolve the actual
     * collection name from the base name and the hint. By default, the collection name
     * is resolved in the following format <code>{collectionName}{separator}{hint}</code>
     * <p>
     * For example, if the base collection name is TEST_COLLECTION, then the resolved collection name when
     * the hint is set to 1 will be TEST_COLLECTION_1
     * <p>
     * This can be overridden by the subclasses to provide custom formatting.
     *
     * @param collectionName Base collection Name
     * @param hint           hint
     * @return the resolved collection name based on the implementation provided in the method.
     */
    public String resolveCollectionName(final String collectionName, final String hint) {
        return String.format("%s%s%s", collectionName, getShardSeparator(), hint);
    }

    /**
     * Provides default implementation to resolve the actual
     * database name from the base name and the hint. By default, the database name
     * is resolved in the following format <code>{databaseName}{separator}{hint}</code>
     * <p>
     * For example, if the database name is TEST_DATABASE with the database hint set to 2,
     * the resolved database name will be TEST_DATABASE_2.
     * <p>
     * This can be overridden by a class to provide different way to format the database name and hint.
     *
     * @param databaseName Base database Name
     * @param hint         hint
     * @return the resolved database name based on the implementation provided in the method.
     */
    public String resolveDatabaseName(final String databaseName, final String hint) {
        return String.format("%s%s%s", databaseName, getShardSeparator(), hint);
    }


    /**
     * Validate if a particular hint is valid for the sharding options or not.
     *
     * @param collectionName Base Collection Name
     * @param hint           hint
     * @return true if the hint passed is valid
     */
    public boolean validateCollectionHint(final String collectionName, final String hint) {
        return hint != null;
    }

    /**
     * Validate if a particular hint is valid for the sharding options or not.
     *
     * @param databaseName Base Database Name
     * @param hint         hint
     * @return true if the hint passed is valid
     */
    public boolean validateDatabaseHint(final String databaseName, final String hint) {
        return hint != null;
    }
}
