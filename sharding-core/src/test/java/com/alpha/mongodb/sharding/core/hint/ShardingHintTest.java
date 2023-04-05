package com.alpha.mongodb.sharding.core.hint;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ShardingHintTest {

    @Test
    public void testShardingHintWithCollectionHint() {
        ShardingHint shardingHint = ShardingHint.withCollectionHint(String.valueOf(0));
        assertNull(shardingHint.getDatabaseHint());
        assertEquals(String.valueOf(0), shardingHint.getCollectionHint());
    }

    @Test
    public void testShardingHintWithDatabaseHint() {
        ShardingHint shardingHint = ShardingHint.withDatabaseHint(String.valueOf(0));
        assertEquals(String.valueOf(0), shardingHint.getDatabaseHint());
        assertNull(shardingHint.getCollectionHint());

    }

    @Test
    public void testShardingHintWithCompositeHint() {
        ShardingHint shardingHint = ShardingHint.withCompositeHint(String.valueOf(0), String.valueOf(0));
        assertEquals(String.valueOf(0), shardingHint.getCollectionHint());
        assertEquals(String.valueOf(0), shardingHint.getDatabaseHint());
    }

    @Test
    public void testCopyConstructorWithNull() {
        ShardingHint shardingHint = null;
        ShardingHint copy = new ShardingHint(shardingHint);
        assertNull(copy.getDatabaseHint());
        assertNull(copy.getCollectionHint());
    }

    @Test
    public void testCopyConstructorWithNonNull() {
        ShardingHint shardingHint = ShardingHint.withCompositeHint(String.valueOf(0), String.valueOf(0));
        ShardingHint copy = new ShardingHint(shardingHint);
        assertEquals(String.valueOf(0), copy.getCollectionHint());
        assertEquals(String.valueOf(0), copy.getDatabaseHint());
    }

}