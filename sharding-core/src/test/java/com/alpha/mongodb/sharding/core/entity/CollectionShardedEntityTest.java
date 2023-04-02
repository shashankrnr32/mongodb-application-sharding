package com.alpha.mongodb.sharding.core.entity;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CollectionShardedEntityTest {

    private static class TestCollectionShardedEntity implements CollectionShardedEntity {

        @Override
        public String resolveCollectionHint() {
            return "0";
        }
    }

    @Test
    public void testCollectionShardedEntity() {
        assertEquals(String.valueOf(0), new TestCollectionShardedEntity().resolveCollectionHint());
    }

}