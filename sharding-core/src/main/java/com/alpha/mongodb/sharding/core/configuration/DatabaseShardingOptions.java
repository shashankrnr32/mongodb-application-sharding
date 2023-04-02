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
 * Database Sharding options. Provides configuration options
 * that are specifically used for Database Sharding strategy.
 *
 * @author Shashank Sharma
 */
@ToString(callSuper = true)
public class DatabaseShardingOptions extends ShardingOptions {

    private final List<String> defaultDatabaseHints;

    /**
     * Default Database Hints in the form of a set to
     * optimize the validation operations
     */
    @Getter
    private final Set<String> defaultDatabaseHintsSet;

    // Derived from other set fields

    /**
     * The optional default database hint
     */
    @Setter
    private String defaultDatabaseHint;

    /**
     * Construct the Database Sharding options using the list of
     * default database hints.
     *
     * @param defaultDatabaseHints Default database hints applicable
     *                             to all the collections using the MongoTemplate
     */
    public DatabaseShardingOptions(List<String> defaultDatabaseHints) {
        this.defaultDatabaseHints = defaultDatabaseHints;
        defaultDatabaseHintsSet = new HashSet<>(defaultDatabaseHints);
    }

    /**
     * A static method to construct DatabaseShardingOptions from
     * an integer stream. This method can be used to create an DatabaseShardingOptions
     * for an integer range, say 0-10
     *
     * <code>
     * DatabaseShardingOptions.withIntegerStreamHints(IntStream.range(0, 10));
     * </code>
     *
     * @param stream Integer Stream
     * @return DatabaseShardingOptions object
     */
    public static DatabaseShardingOptions withIntegerStreamHints(IntStream stream) {
        return new DatabaseShardingOptions(stream.mapToObj(String::valueOf).collect(Collectors.toList()));
    }

    /**
     * Get the default database hint if set. If the default hint
     * is not set, then the first hint in the list of hints will be considered
     * for default.
     *
     * @return default hint
     */
    public String getDefaultDatabaseHint() {
        if (defaultDatabaseHint == null) {
            return defaultDatabaseHints.get(0);
        } else {
            return defaultDatabaseHint;
        }
    }
}
