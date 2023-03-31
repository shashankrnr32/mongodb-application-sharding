package com.alpha.mongodb.sharding.core.exception;

import lombok.experimental.StandardException;

@StandardException
public class UnresolvableCollectionShardException extends UnresolvableShardException {
    public UnresolvableCollectionShardException() {
        super("Cannot resolve collection shard");
    }
}
