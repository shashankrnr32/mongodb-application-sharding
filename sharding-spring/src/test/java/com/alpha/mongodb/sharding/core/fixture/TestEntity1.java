package com.alpha.mongodb.sharding.core.fixture;

import com.alpha.mongodb.sharding.core.callback.HintResolutionCallback;
import com.alpha.mongodb.sharding.core.hint.ShardingHint;
import lombok.Data;
import lombok.experimental.FieldNameConstants;
import org.bson.Document;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;

@Data
@org.springframework.data.mongodb.core.mapping.Document("TEST1")
@FieldNameConstants
public class TestEntity1 {
    @Id
    private String id;

    @Indexed(unique = true)
    private String indexedField;

    public static class TestEntity1HintResolutionCallback implements HintResolutionCallback<TestEntity1> {

        @Override
        public ShardingHint resolveHintForFindContext(Document query, Class<TestEntity1> entityClass) {
            return ShardingHint.withCollectionHint("0");
        }

        @Override
        public ShardingHint resolveHintForSaveContext(TestEntity1 entity) {
            return ShardingHint.withCollectionHint("0");
        }
    }
}