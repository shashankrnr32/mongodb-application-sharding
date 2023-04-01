package com.alpha.mongodb.sharding.core.exception;

import lombok.experimental.StandardException;

@StandardException
public class UnresolvableShardException extends IllegalStateException {

    public UnresolvableShardException() {
        super("Cannot resolve shard");
    }

    public UnresolvableShardException(final String message) {
        super(message);
    }
}
