package com.alpha.mongodb.sharding.core.hint;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.springframework.lang.Nullable;

@Data
@NoArgsConstructor
public class ShardingHint {

    @Getter(onMethod = @__(@Nullable))
    private String databaseHint;

    @Getter(onMethod = @__(@Nullable))
    private String collectionHint;

    ShardingHint(@Nullable ShardingHint copy) {
        setDatabaseHint(copy.getDatabaseHint());
        setCollectionHint(copy.getCollectionHint());
    }
}