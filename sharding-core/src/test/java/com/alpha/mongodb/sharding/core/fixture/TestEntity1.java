package com.alpha.mongodb.sharding.core.fixture;

import com.alpha.mongodb.sharding.core.callback.IHintResolutionCallback;
import com.alpha.mongodb.sharding.core.hint.ShardingHint;
import lombok.Data;
import lombok.experimental.FieldNameConstants;

import java.util.Map;

@Data
@FieldNameConstants
public class TestEntity1 {
    private String id;

    private String indexedField;

    public static class TestEntity1HintResolutionCallback implements IHintResolutionCallback {

        public ShardingHint resolveHintForFindContext(Map<String, Object> query, Class<TestEntity1> entityClass) {
            return ShardingHint.withCollectionHint("0");
        }

        public ShardingHint resolveHintForSaveContext(TestEntity1 entity) {
            return ShardingHint.withCollectionHint("0");
        }
    }
}