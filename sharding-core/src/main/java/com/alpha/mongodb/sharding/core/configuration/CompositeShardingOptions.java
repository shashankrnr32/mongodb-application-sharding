package com.alpha.mongodb.sharding.core.configuration;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Composite sharding options
 *
 * @author SHashank Sharma
 */
@ToString(callSuper = true)
public class CompositeShardingOptions extends DatabaseShardingOptions {

    @Getter(AccessLevel.PACKAGE)
    private final List<String> defaultCollectionHints;

    @Getter
    private final Set<String> defaultCollectionHintsSet;

    @Setter
    private String defaultCollectionHint;

    // Derived from other set fields
    @Getter
    private Map<String, List<String>> collectionHintsMapList = new HashMap<>();

    private DelegatedCollectionShardingOptions delegatedCollectionShardingOptions;

    @Setter
    @Getter
    private Map<String, Set<String>> collectionHintsMapSet = new HashMap<>();

    public CompositeShardingOptions(final List<String> defaultDatabaseHints, final List<String> defaultCollectionHints) {
        super(defaultDatabaseHints);
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

    public void setCollectionHintsMapList(Map<String, List<String>> collectionHintsMapList) {
        this.collectionHintsMapList = collectionHintsMapList;
        collectionHintsMapSet = new HashMap<>();
        collectionHintsMapList.forEach((collectionName, hints) -> {
            collectionHintsMapSet.put(collectionName, new HashSet<>(hints));
        });
    }

    @Override
    public boolean validateCollectionHint(String collectionName, String hint) {
        if (!super.validateCollectionHint(collectionName, hint)) {
            return false;
        }

        Set<String> validCollectionHints = this.getCollectionHintsMapSet().getOrDefault(
                collectionName, getDefaultCollectionHintsSet());

        return validCollectionHints.contains(hint);
    }

    public DelegatedCollectionShardingOptions getDelegatedCollectionShardingOptions() {
        if (delegatedCollectionShardingOptions == null) {
            delegatedCollectionShardingOptions = new DelegatedCollectionShardingOptions(this);
        }
        return delegatedCollectionShardingOptions;
    }

    public static CompositeShardingOptions withIntegerStreamHints(IntStream databaseHintStream, IntStream collectionHintStream) {
        return new CompositeShardingOptions(databaseHintStream.mapToObj(String::valueOf).collect(Collectors.toList()),
                collectionHintStream.mapToObj(String::valueOf).collect(Collectors.toList()));
    }
}
