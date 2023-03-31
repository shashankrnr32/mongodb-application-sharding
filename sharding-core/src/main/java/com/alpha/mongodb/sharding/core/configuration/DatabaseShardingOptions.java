package com.alpha.mongodb.sharding.core.configuration;

import lombok.Data;
import lombok.ToString;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Data
@ToString(callSuper = true)
public class DatabaseShardingOptions extends ShardingOptions {
    private List<String> databaseHints;

    private String defaultDatabaseHint;

    public String getDefaultDatabaseHint() {
        if (defaultDatabaseHint == null) {
            return databaseHints.get(0);
        } else {
            return defaultDatabaseHint;
        }
    }

    public void setIntStreamHints(IntStream stream) {
        if (CollectionUtils.isEmpty(databaseHints)) {
            databaseHints = new ArrayList<>();
        }
        this.databaseHints.addAll(stream.mapToObj(String::valueOf).collect(Collectors.toList()));
    }
}
