package com.alpha.mongodb.sharding.core.fixtures;

import com.alpha.mongodb.sharding.core.callback.HintResolutionCallback;
import com.alpha.mongodb.sharding.core.hint.ShardingHint;
import lombok.Data;
import org.bson.Document;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.callback.EntityCallback;
import org.springframework.data.mongodb.core.index.Indexed;

@Data
@org.springframework.data.mongodb.core.mapping.Document("TEST2")
public class TestEntity2 {
    @Id
    private String id;

    @Indexed(unique = true)
    private String indexedField;

    public static class TestEntity2HintResolutionCallback implements
            EntityCallback<TestEntity2>, HintResolutionCallback<TestEntity2> {

        @Override
        public ShardingHint resolveHintForFindContext(Document query, Class<TestEntity2> entityClass) {
            return ShardingHint.withCollectionHint("0");
        }

        @Override
        public ShardingHint resolveHintForSaveContext(TestEntity2 entity) {
            return ShardingHint.withCollectionHint("0");
        }
    }
}