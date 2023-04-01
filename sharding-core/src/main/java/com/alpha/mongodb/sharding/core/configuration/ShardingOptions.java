package com.alpha.mongodb.sharding.core.configuration;

import lombok.Data;

/**
 * Base class for Sharding options
 *
 * @author Shashank Sharma
 */
@Data
public class ShardingOptions {
    private String shardSeparator = "_";
}
