package com.alpha.mongodb.sharding.core.configuration;

import lombok.Data;

@Data
public class ShardingOptions {
    private String shardSeparator = "_";
}
