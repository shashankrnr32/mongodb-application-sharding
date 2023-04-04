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
        if (copy == null) {
            return;
        }
        setDatabaseHint(copy.getDatabaseHint());
        setCollectionHint(copy.getCollectionHint());
    }

    public static ShardingHint withCollectionHint(String collectionHint) {
        ShardingHint shardingHint = new ShardingHint();
        shardingHint.setCollectionHint(collectionHint);
        return shardingHint;
    }

    public static ShardingHint withDatabaseHint(String databaseHint) {
        ShardingHint shardingHint = new ShardingHint();
        shardingHint.setDatabaseHint(databaseHint);
        return shardingHint;
    }

    public static ShardingHint withCompositeHint(String databaseHint, String collectionHint) {
        ShardingHint shardingHint = new ShardingHint();
        shardingHint.setDatabaseHint(databaseHint);
        shardingHint.setCollectionHint(collectionHint);
        return shardingHint;
    }
}