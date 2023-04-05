package com.alpha.mongodb.sharding.core.hint;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class ShardingHintManagerTest {

    @Before
    public void setup() {
        ShardingHintManager.clear();
    }

    @Test
    public void testSetDatabaseHintWhenNotSet() {
        ShardingHintManager.setDatabaseHint(String.valueOf(0));
        assertEquals(String.valueOf(0), ShardingHintManager.getHint().get().getDatabaseHint());
        assertNull(ShardingHintManager.getHint().get().getCollectionHint());
    }

    @Test(expected = IllegalStateException.class)
    public void testSetDatabaseHintWhenAlreadySet() {
        ShardingHintManager.setDatabaseHint(String.valueOf(0));
        assertEquals(String.valueOf(0), ShardingHintManager.getHint().get().getDatabaseHint());
        assertNull(ShardingHintManager.getHint().get().getCollectionHint());

        ShardingHintManager.setDatabaseHint("2");
    }

    @Test
    public void testSetCollectionHintWhenNotSet() {
        ShardingHintManager.setCollectionHint(String.valueOf(0));
        assertEquals(String.valueOf(0), ShardingHintManager.getHint().get().getCollectionHint());
        assertNull(ShardingHintManager.getHint().get().getDatabaseHint());
    }

    @Test(expected = IllegalStateException.class)
    public void testSetCollectionHintWhenAlreadySet() {
        ShardingHintManager.setCollectionHint(String.valueOf(0));
        assertEquals(String.valueOf(0), ShardingHintManager.getHint().get().getCollectionHint());
        assertNull(ShardingHintManager.getHint().get().getDatabaseHint());

        ShardingHintManager.setCollectionHint("2");
    }

    @Test
    public void testSetCompositeHintWhenNotSet() {
        ShardingHintManager.setCompositeHint(String.valueOf(0), String.valueOf(0));
        assertEquals(String.valueOf(0), ShardingHintManager.getHint().get().getCollectionHint());
        assertEquals(String.valueOf(0), ShardingHintManager.getHint().get().getDatabaseHint());
    }

    @Test(expected = IllegalStateException.class)
    public void testSetCompositeHintWhenAlreadySet() {
        ShardingHintManager.setCompositeHint(String.valueOf(0), String.valueOf(0));
        assertEquals(String.valueOf(0), ShardingHintManager.getHint().get().getCollectionHint());
        assertEquals(String.valueOf(0), ShardingHintManager.getHint().get().getDatabaseHint());

        ShardingHintManager.setCompositeHint(String.valueOf(2), String.valueOf(0));
    }

    @Test
    public void testSetDatabaseHintWhenCollectionHintIsSet() {
        ShardingHintManager.setCollectionHint(String.valueOf(0));
        assertEquals(String.valueOf(0), ShardingHintManager.getHint().get().getCollectionHint());
        assertNull(ShardingHintManager.getHint().get().getDatabaseHint());

        ShardingHintManager.setDatabaseHint(String.valueOf(0));
        assertEquals(String.valueOf(0), ShardingHintManager.getHint().get().getDatabaseHint());
    }


    @Test
    public void testSetCollectionHintWhenDatabaseHintIsSet() {
        ShardingHintManager.setDatabaseHint(String.valueOf(0));
        assertEquals(String.valueOf(0), ShardingHintManager.getHint().get().getDatabaseHint());
        assertNull(ShardingHintManager.getHint().get().getCollectionHint());

        ShardingHintManager.setCollectionHint(String.valueOf(0));
        assertEquals(String.valueOf(0), ShardingHintManager.getHint().get().getCollectionHint());
    }

    @Test
    public void getShardingHintWhenNotSet() {
        assertFalse(ShardingHintManager.getHint().isPresent());
    }

}