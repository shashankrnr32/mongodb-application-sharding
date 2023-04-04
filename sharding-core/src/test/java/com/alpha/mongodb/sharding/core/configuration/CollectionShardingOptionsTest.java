package com.alpha.mongodb.sharding.core.configuration;

import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CollectionShardingOptionsTest {

    @Test
    public void testCollectionShardingOptions() {
        CollectionShardingOptions collectionShardingOptions =
                CollectionShardingOptions.withIntegerStreamHints(IntStream.range(0, 3));
        assertEquals(String.valueOf(0), collectionShardingOptions.getDefaultCollectionHint());
        assertEquals(3, collectionShardingOptions.getDefaultCollectionHintsSet().size());
        assertTrue(collectionShardingOptions.getCollectionHintsMapList().isEmpty());

        Map<String, List<String>> collectionHints = new java.util.HashMap<>();
        collectionHints.put("TEST_COLLECTION", IntStream.range(0, 3).mapToObj(String::valueOf).collect(Collectors.toList()));
        collectionHints.put("SOME_COLLECTION", IntStream.range(0, 4).mapToObj(String::valueOf).collect(Collectors.toList()));
        collectionShardingOptions.setCollectionHintsMapList(collectionHints);
        assertEquals(4, collectionShardingOptions.getCollectionHintsMapList().get("SOME_COLLECTION").size());
        assertEquals(3, collectionShardingOptions.getCollectionHintsMapList().get("TEST_COLLECTION").size());

        collectionShardingOptions.setDefaultCollectionHint(String.valueOf(2));
        assertEquals(String.valueOf(2), collectionShardingOptions.getDefaultCollectionHint());

        assertEquals("TEST_COLLECTION_2", collectionShardingOptions.resolveCollectionName("TEST_COLLECTION", String.valueOf(2)));

        // Coverage
        String toStringVal = collectionShardingOptions.toString();
        assertNotNull(toStringVal);
    }

}