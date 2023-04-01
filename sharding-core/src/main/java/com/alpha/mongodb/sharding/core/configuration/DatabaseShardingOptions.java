package com.alpha.mongodb.sharding.core.configuration;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Database Sharding options
 *
 * @author Shashank Sharma
 */
@ToString(callSuper = true)
public class DatabaseShardingOptions extends ShardingOptions {

    private final List<String> defaultDatabaseHints;
    @Getter
    private final Set<String> defaultDatabaseHintsSet;

    // Derived from other set fields
    @Setter
    private String defaultDatabaseHint;

    public DatabaseShardingOptions(List<String> defaultDatabaseHints) {
        this.defaultDatabaseHints = defaultDatabaseHints;
        defaultDatabaseHintsSet = new HashSet<>(defaultDatabaseHints);
    }

    public static DatabaseShardingOptions withIntegerStreamHints(IntStream stream) {
        return new DatabaseShardingOptions(stream.mapToObj(String::valueOf).collect(Collectors.toList()));
    }

    public String getDefaultDatabaseHint() {
        if (defaultDatabaseHint == null) {
            return defaultDatabaseHints.get(0);
        } else {
            return defaultDatabaseHint;
        }
    }
}
