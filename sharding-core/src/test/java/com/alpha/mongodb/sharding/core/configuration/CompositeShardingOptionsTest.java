package com.alpha.mongodb.sharding.core.configuration;

import com.alpha.mongodb.sharding.core.fixtures.TestEntity1;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CompositeShardingOptionsTest {

    @Test
    public void testCompositeShardingOptions() {
        CompositeShardingOptions compositeShardingOptions =
                CompositeShardingOptions.withIntegerStreamHints(IntStream.range(0, 3), IntStream.range(0, 3));
        assertEquals(String.valueOf(0), compositeShardingOptions.getDefaultDatabaseHint());
        assertEquals(3, compositeShardingOptions.getDefaultDatabaseHintsSet().size());
        assertEquals(String.valueOf(0), compositeShardingOptions.getDefaultCollectionHint());
        assertEquals(3, compositeShardingOptions.getDefaultCollectionHintsSet().size());
        assertTrue(compositeShardingOptions.getCollectionHintsMapList().isEmpty());


        Map<String, List<String>> collectionHints = new java.util.HashMap<>();
        collectionHints.put("TEST_COLLECTION", IntStream.range(0, 3).mapToObj(String::valueOf).collect(Collectors.toList()));
        collectionHints.put("SOME_COLLECTION", IntStream.range(0, 4).mapToObj(String::valueOf).collect(Collectors.toList()));
        compositeShardingOptions.setCollectionHintsMapList(collectionHints);
        assertEquals(4, compositeShardingOptions.getCollectionHintsMapList().get("SOME_COLLECTION").size());
        assertEquals(3, compositeShardingOptions.getCollectionHintsMapList().get("TEST_COLLECTION").size());

        compositeShardingOptions.setDefaultDatabaseHint(String.valueOf(2));
        assertEquals(String.valueOf(2), compositeShardingOptions.getDefaultDatabaseHint());
        compositeShardingOptions.setDefaultCollectionHint(String.valueOf(2));
        assertEquals(String.valueOf(2), compositeShardingOptions.getDefaultCollectionHint());

        assertEquals("TEST_DB_2", compositeShardingOptions.resolveDatabaseName("TEST_DB", String.valueOf(2)));
        assertEquals("TEST_COLLECTION_2", compositeShardingOptions.resolveCollectionName("TEST_COLLECTION", String.valueOf(2)));

        // Coverage
        String toStringVal = compositeShardingOptions.toString();
        assertNotNull(toStringVal);

        assertTrue(compositeShardingOptions.validateDatabaseHint("TEST_DB", String.valueOf(2)));
        assertFalse(compositeShardingOptions.validateDatabaseHint("TEST_DB", String.valueOf(5)));
        assertFalse(compositeShardingOptions.validateDatabaseHint("TEST_DB", null));
        assertTrue(compositeShardingOptions.validateCollectionHint("TEST_COLLECTION", String.valueOf(1)));
        assertFalse(compositeShardingOptions.validateCollectionHint("TEST_COLLECTION", String.valueOf(5)));
        assertFalse(compositeShardingOptions.validateCollectionHint("TEST_COLLECTION", null));

        TestEntity1.TestEntity1HintResolutionCallback callback = new TestEntity1.TestEntity1HintResolutionCallback();
        compositeShardingOptions.setHintResolutionCallbacks(Collections.singleton(callback));
        assertEquals(1, compositeShardingOptions.getHintResolutionCallbacks().size());

    }
}
