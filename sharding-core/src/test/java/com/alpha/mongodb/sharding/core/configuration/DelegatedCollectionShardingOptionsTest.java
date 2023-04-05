package com.alpha.mongodb.sharding.core.configuration;

import org.junit.Test;

import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class DelegatedCollectionShardingOptionsTest {

    @Test
    public void testDelegatedCollectionShardingOptions() {
        CompositeShardingOptions compositeShardingOptions = CompositeShardingOptions.withIntegerStreamHints(
                IntStream.range(0, 3), IntStream.range(0, 3));
        assertEquals(compositeShardingOptions.validateCollectionHint("TEST_COLLECTION", String.valueOf(1)),
                compositeShardingOptions.getDelegatedCollectionShardingOptions().validateCollectionHint("TEST_COLLECTION", String.valueOf(1)));
        assertEquals(compositeShardingOptions.validateCollectionHint("TEST_COLLECTION", null),
                compositeShardingOptions.getDelegatedCollectionShardingOptions().validateCollectionHint("TEST_COLLECTION", null));
        assertEquals(compositeShardingOptions.validateDatabaseHint("TEST_DATABASE", String.valueOf(1)),
                compositeShardingOptions.getDelegatedCollectionShardingOptions().validateDatabaseHint("TEST_DATABASE", String.valueOf(1)));
        assertEquals(compositeShardingOptions.resolveCollectionName("TEST_COLLECTION", String.valueOf(1)),
                compositeShardingOptions.getDelegatedCollectionShardingOptions().resolveCollectionName("TEST_COLLECTION", String.valueOf(1)));
        assertEquals(compositeShardingOptions.resolveDatabaseName("TEST_DATABASE", String.valueOf(1)),
                compositeShardingOptions.getDelegatedCollectionShardingOptions().resolveDatabaseName("TEST_DATABASE", String.valueOf(1)));
    }

    @Test
    public void testDelegatedCollectionShardingOptionsWithConstructor() {
        CompositeShardingOptions compositeShardingOptions = CompositeShardingOptions.withIntegerStreamHints(
                IntStream.range(0, 3), IntStream.range(0, 3));
        DelegatedCollectionShardingOptions delegatedCollectionShardingOptions = new DelegatedCollectionShardingOptions(compositeShardingOptions);
        assertEquals(compositeShardingOptions.validateCollectionHint("TEST_COLLECTION", String.valueOf(1)),
                delegatedCollectionShardingOptions.validateCollectionHint("TEST_COLLECTION", String.valueOf(1)));
        assertEquals(compositeShardingOptions.validateCollectionHint("TEST_COLLECTION", null),
                delegatedCollectionShardingOptions.validateCollectionHint("TEST_COLLECTION", null));
        assertEquals(compositeShardingOptions.validateDatabaseHint("TEST_DATABASE", String.valueOf(1)),
                delegatedCollectionShardingOptions.validateDatabaseHint("TEST_DATABASE", String.valueOf(1)));
        assertEquals(compositeShardingOptions.resolveCollectionName("TEST_COLLECTION", String.valueOf(1)),
                delegatedCollectionShardingOptions.resolveCollectionName("TEST_COLLECTION", String.valueOf(1)));
        assertEquals(compositeShardingOptions.resolveDatabaseName("TEST_DATABASE", String.valueOf(1)),
                delegatedCollectionShardingOptions.resolveDatabaseName("TEST_DATABASE", String.valueOf(1)));


    }

}