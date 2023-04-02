package com.alpha.mongodb.sharding.core.configuration;

import org.junit.Test;

import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DatabaseShardingOptionsTest {

    @Test
    public void testDatabaseShardingOptions() {
        DatabaseShardingOptions databaseShardingOptions =
                DatabaseShardingOptions.withIntegerStreamHints(IntStream.range(0, 3));
        assertEquals(String.valueOf(0), databaseShardingOptions.getDefaultDatabaseHint());
        assertEquals(3, databaseShardingOptions.getDefaultDatabaseHintsSet().size());

        databaseShardingOptions.setDefaultDatabaseHint(String.valueOf(2));
        assertEquals(String.valueOf(2), databaseShardingOptions.getDefaultDatabaseHint());

        assertEquals("TEST_DB_2", databaseShardingOptions.resolveDatabaseName("TEST_DB", String.valueOf(2)));

        // Coverage
        String toStringVal = databaseShardingOptions.toString();
        assertNotNull(toStringVal);
    }

}