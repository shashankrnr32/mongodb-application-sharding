package com.alpha.mongodb.sharding.core.exception;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UnresolvableShardExceptionTest {

    @Test
    public void testUnresolvableShardException() {
        assertEquals("Cannot resolve shard", new UnresolvableShardException().getMessage());
    }

}