package com.alpha.mongodb.sharding.core.entity;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DatabaseShardedEntityTest {

    @Test
    public void testCollectionShardedEntity() {
        assertEquals(String.valueOf(0), new TestDatabaseShardedEntity().resolveDatabaseHint());
    }

    private static class TestDatabaseShardedEntity implements DatabaseShardedEntity {

        @Override
        public String resolveDatabaseHint() {
            return "0";
        }
    }

}