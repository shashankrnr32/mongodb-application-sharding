package com.alpha.mongodb.sharding.core.exception;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UnresolvableDatabaseShardExceptionTest {

    @Test
    public void testUnresolvableDatabaseShardException() {
        assertEquals("Cannot resolve database shard", new UnresolvableDatabaseShardException().getMessage());
    }

}