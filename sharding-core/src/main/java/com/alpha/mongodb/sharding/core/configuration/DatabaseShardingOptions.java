package com.alpha.mongodb.sharding.core.configuration;

import lombok.Data;

import java.util.List;

@Data
public class DatabaseShardingOptions extends ShardingOptions {
    private List<String> databaseHints;

    private String defaultDatabaseHint;

    public String getDefaultDatabaseHint() {
        if (defaultDatabaseHint == null) {
            return databaseHints.get(0);
        } else {
            return defaultDatabaseHint;
        }
    }
}
