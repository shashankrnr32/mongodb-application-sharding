package com.alpha.mongodb.sharding.core.configuration;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
public class ShardingOptions {
    private String shardSeparator = "_";


    @Setter
    @Getter
    private boolean resolveToDefaultHint = false;
}
