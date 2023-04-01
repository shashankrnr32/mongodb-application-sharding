package com.alpha.mongodb.sharding.core.configuration;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * Collection sharding options
 *
 * @author SHashank Sharma
 */
@ToString(callSuper = true)
public class CollectionShardingOptions extends ShardingOptions {

    private final List<String> defaultCollectionHints;

    @Setter
    private String defaultCollectionHint;

    @Getter
    private Map<String, List<String>> collectionHints = new HashMap<>();

    // Derived from other set fields

    @Getter
    private final Set<String> defaultCollectionHintsSet;

    @Setter
    @Getter
    private Map<String, Set<String>> collectionHintsSet = new HashMap<>();

    public CollectionShardingOptions(List<String> defaultCollectionHints) {
        this.defaultCollectionHints = defaultCollectionHints;
        defaultCollectionHintsSet = new HashSet<>(defaultCollectionHints);
    }

    public String getDefaultCollectionHint() {
        if (defaultCollectionHint == null) {
            return defaultCollectionHints.get(0);
        } else {
            return defaultCollectionHint;
        }
    }

    public static CollectionShardingOptions withIntegerStreamHints(IntStream stream) {
        return new CollectionShardingOptions(stream.mapToObj(String::valueOf).collect(Collectors.toList()));
    }

    public void setCollectionHints(Map<String, List<String>> collectionHints) {
        this.collectionHints = collectionHints;
        collectionHintsSet = new HashMap<>();
        collectionHints.forEach((collectionName, hints) -> {
            collectionHintsSet.put(collectionName, new HashSet<>(hints));
        });
    }
}
