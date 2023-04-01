package com.alpha.mongodb.sharding.core.configuration;

import org.junit.Test;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CollectionShardingOptionsTest {

    @Test
    public void testAllShardingOptions() {
        CollectionShardingOptions collectionShardingOptions =
                CollectionShardingOptions.withIntegerStreamHints(IntStream.range(0, 3));
        assertEquals(3, collectionShardingOptions.getDefaultCollectionHintsSet().size());
        assertTrue(collectionShardingOptions.getCollectionHints().isEmpty());

        collectionShardingOptions.setCollectionHints(Map.of(
                "TEST_COLLECTION", IntStream.range(0, 3).mapToObj(String::valueOf).collect(Collectors.toList()),
                "SOME_COLLECTION", IntStream.range(0, 4).mapToObj(String::valueOf).collect(Collectors.toList())
        ));
        assertEquals(4, collectionShardingOptions.getCollectionHints().get("SOME_COLLECTION").size());
    }

}