package com.alpha.mongodb.sharding.core.exception;

import lombok.experimental.StandardException;

@StandardException
public class UnresolvableDatabaseShardException extends UnresolvableShardException {
    public UnresolvableDatabaseShardException() {
        super("Cannot resolve database shard");
    }
}
