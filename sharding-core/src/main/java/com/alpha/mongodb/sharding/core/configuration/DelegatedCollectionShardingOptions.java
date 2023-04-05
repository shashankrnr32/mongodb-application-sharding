package com.alpha.mongodb.sharding.core.configuration;

import lombok.EqualsAndHashCode;

/**
 * Delegated Collection Sharding options that is used with CompositeShardingOptions.
 * Use {@link CollectionShardingOptions} for defining sharding options for a collection
 * sharded schema.
 *
 * @author Shashank Sharma
 * @see com.alpha.mongodb.sharding.core.configuration.CollectionShardingOptions
 */
@EqualsAndHashCode(callSuper = true)
public class DelegatedCollectionShardingOptions extends CollectionShardingOptions {

    private final CompositeShardingOptions delegate;

    public DelegatedCollectionShardingOptions(CompositeShardingOptions compositeShardingOptions) {
        super(compositeShardingOptions.getDefaultCollectionHints());
        this.delegate = compositeShardingOptions;
        this.setCollectionHintsMapList(compositeShardingOptions.getCollectionHintsMapList());
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