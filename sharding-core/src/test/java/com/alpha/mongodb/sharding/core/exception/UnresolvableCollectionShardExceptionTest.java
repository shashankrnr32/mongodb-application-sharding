package com.alpha.mongodb.sharding.core.exception;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UnresolvableCollectionShardExceptionTest {

    @Test
    public void testUnresolvableCollectionShardException() {
        assertEquals("Cannot resolve collection shard", new UnresolvableCollectionShardException().getMessage());
    }

}