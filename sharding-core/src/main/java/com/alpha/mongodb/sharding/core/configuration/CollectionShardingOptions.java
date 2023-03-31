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
public class CollectionShardingOptions extends ShardingOptions {
    private List<String> collectionHints;

    private String defaultCollectionHint;

    public String getDefaultCollectionHint() {
        if (defaultCollectionHint == null) {
            return collectionHints.get(0);
        } else {
            return defaultCollectionHint;
        }
    }

    public void setIntStreamHints(IntStream stream) {
        if (CollectionUtils.isEmpty(collectionHints)) {
            collectionHints = new ArrayList<>();
        }
        this.collectionHints.addAll(stream.mapToObj(String::valueOf).collect(Collectors.toList()));
    }
}
