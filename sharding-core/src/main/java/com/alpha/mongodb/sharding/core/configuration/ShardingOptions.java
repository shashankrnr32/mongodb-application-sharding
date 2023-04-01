package com.alpha.mongodb.sharding.core.configuration;

import lombok.Data;

/**
 * Base class for Sharding options
 *
 * @author Shashank Sharma
 */
@Data
public class ShardingOptions {
    private String shardSeparator = "_";

    public String resolveCollectionName(final String collectionName, final String hint) {
        return String.format("%s%s%s", collectionName, getShardSeparator(), hint);
    }

    public String resolveDatabaseName(final String databaseName, final String hint) {
        return String.format("%s%s%s", databaseName, getShardSeparator(), hint);
    }
}
