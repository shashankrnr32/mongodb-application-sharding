package com.alpha.mongodb.sharding.core.configuration;

import lombok.Data;

import java.util.List;

@Data
public class CollectionShardingOptions extends ShardingOptions {
    private List<String> collectionHints;

    private String defaultCollectionHint;

    public String getDefaultCollectionHint() {
        if (defaultCollectionHint == null) {
            return collectionHints.get(0);
        } else {
            return defaultCollectionHint;
        }
    }
}
