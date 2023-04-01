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
    @Getter
    private final Set<String> defaultCollectionHintsSet;
    @Setter
    private String defaultCollectionHint;

    // Derived from other set fields
    @Getter
    private Map<String, List<String>> collectionHints = new HashMap<>();
    @Getter
    private Map<String, Set<String>> collectionHintsSet = new HashMap<>();

    public CollectionShardingOptions(List<String> defaultCollectionHints) {
        this.defaultCollectionHints = defaultCollectionHints;
        defaultCollectionHintsSet = new HashSet<>(defaultCollectionHints);
    }

    public static CollectionShardingOptions withIntegerStreamHints(IntStream stream) {
        return new CollectionShardingOptions(stream.mapToObj(String::valueOf).collect(Collectors.toList()));
    }

    public String getDefaultCollectionHint() {
        if (defaultCollectionHint == null) {
            return defaultCollectionHints.get(0);
        } else {
            return defaultCollectionHint;
        }
    }

    public void setCollectionHints(Map<String, List<String>> collectionHints) {
        this.collectionHints = collectionHints;
        collectionHintsSet = new HashMap<>();
        collectionHints.forEach((collectionName, hints) -> {
            collectionHintsSet.put(collectionName, new HashSet<>(hints));
        });
    }
}
