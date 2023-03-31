package com.alpha.mongodb.sharding.core.configuration;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@ToString(callSuper = true)
public class DatabaseShardingOptions extends ShardingOptions {

    private final List<String> defaultDatabaseHints;

    @Setter
    private String defaultDatabaseHint;

    // Derived from other set fields

    @Getter
    private final Set<String> databaseHintsSet;

    public DatabaseShardingOptions(List<String> defaultDatabaseHints) {
        this.defaultDatabaseHints = defaultDatabaseHints;
        databaseHintsSet = new HashSet<>(defaultDatabaseHints);
    }

    public String getDefaultDatabaseHint() {
        if (defaultDatabaseHint == null) {
            return defaultDatabaseHints.get(0);
        } else {
            return defaultDatabaseHint;
        }
    }

    public static DatabaseShardingOptions withIntegerStreamHints(IntStream stream) {
        return new DatabaseShardingOptions(stream.mapToObj(String::valueOf).collect(Collectors.toList()));
    }
}
