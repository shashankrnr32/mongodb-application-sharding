package com.alpha.mongodb.sharding.core.configuration;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Composite sharding options
 *
 * @author SHashank Sharma
 */
@ToString(callSuper = true)
public class CompositeShardingOptions extends DatabaseShardingOptions {

    private final List<String> defaultCollectionHints;
    @Getter
    private final Set<String> defaultCollectionHintsSet;
    @Setter
    private String defaultCollectionHint;

    // Derived from other set fields
    @Getter
    private Map<String, List<String>> collectionHints = new HashMap<>();
    private DelegatedCollectionShardingOptions delegatedCollectionShardingOptions;
    @Setter
    @Getter
    private Map<String, Set<String>> collectionHintsSet = new HashMap<>();

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

    public void setCollectionHints(Map<String, List<String>> collectionHints) {
        this.collectionHints = collectionHints;
        collectionHintsSet = new HashMap<>();
        collectionHints.forEach((collectionName, hints) -> {
            collectionHintsSet.put(collectionName, new HashSet<>(hints));
        });
    }

    public DelegatedCollectionShardingOptions getDelegatedCollectionShardingOptions() {
        if (delegatedCollectionShardingOptions == null) {
            delegatedCollectionShardingOptions = new DelegatedCollectionShardingOptions(this);
        }
        return delegatedCollectionShardingOptions;
    }

    public static class DelegatedCollectionShardingOptions extends CollectionShardingOptions {

        private final CompositeShardingOptions delegate;

        public DelegatedCollectionShardingOptions(CompositeShardingOptions compositeShardingOptions) {
            super(compositeShardingOptions.defaultCollectionHints);
            this.delegate = compositeShardingOptions;
            this.setCollectionHintsMapList(compositeShardingOptions.getCollectionHints());
            this.setDefaultCollectionHint(compositeShardingOptions.getDefaultCollectionHint());
            this.setShardSeparator(compositeShardingOptions.getShardSeparator());
        }

        @Override
        public String resolveDatabaseName(String databaseName, String hint) {
            return delegate.resolveDatabaseName(databaseName, hint);
        }

        @Override
        public String resolveCollectionName(String collectionName, String hint) {
            return delegate.resolveCollectionName(collectionName, hint);
        }

        @Override
        public boolean validateCollectionHint(String collectionName, String hint) {
            return delegate.validateCollectionHint(collectionName, hint);
        }

        @Override
        public boolean validateDatabaseHint(String databaseName, String hint) {
            return delegate.validateDatabaseHint(databaseName, hint);
        }
    }
}
