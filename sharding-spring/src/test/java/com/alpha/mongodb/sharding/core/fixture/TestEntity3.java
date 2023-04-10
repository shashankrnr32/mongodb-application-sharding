package com.alpha.mongodb.sharding.core.fixture;

import com.alpha.mongodb.sharding.core.callback.CollectionShardedEntityHintResolutionCallback;
import com.alpha.mongodb.sharding.core.callback.CompositeShardedEntityHintResolutionCallback;
import com.alpha.mongodb.sharding.core.callback.DatabaseShardedEntityHintResolutionCallback;
import com.alpha.mongodb.sharding.core.entity.CollectionShardedEntity;
import com.alpha.mongodb.sharding.core.entity.CompositeShardedEntity;
import com.alpha.mongodb.sharding.core.entity.DatabaseShardedEntity;
import com.alpha.mongodb.sharding.core.hint.ShardingHint;
import lombok.Data;
import lombok.experimental.FieldNameConstants;
import org.bson.Document;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;

@Data
@org.springframework.data.mongodb.core.mapping.Document("TEST3")
@FieldNameConstants
public class TestEntity3 implements CollectionShardedEntity, DatabaseShardedEntity, CompositeShardedEntity {
    @Id
    private String id;

    @Indexed(unique = true)
    private String indexedField;

    @Override
    public String resolveCollectionHint() {
        return String.valueOf(0);
    }

    @Override
    public String resolveDatabaseHint() {
        return String.valueOf(0);
    }

    public static class TestEntity3DatabaseHintResolutionCallback implements
            DatabaseShardedEntityHintResolutionCallback<TestEntity3> {

        @Override
        public ShardingHint resolveHintForFindContext(Document query, Class<TestEntity3> entityClass) {
            return ShardingHint.withDatabaseHint(String.valueOf(0));
        }
    }

    public static class TestEntity3CollectionHintResolutionCallback implements
            CollectionShardedEntityHintResolutionCallback<TestEntity3> {

        @Override
        public ShardingHint resolveHintForFindContext(Document query, Class<TestEntity3> entityClass) {
            return ShardingHint.withCollectionHint(String.valueOf(0));
        }
    }

    public static class TestEntity3CompositeHintResolutionCallback implements
            CompositeShardedEntityHintResolutionCallback<TestEntity3> {

        @Override
        public ShardingHint resolveHintForFindContext(Document query, Class<TestEntity3> entityClass) {
            return ShardingHint.withCollectionHint(String.valueOf(0));
        }
    }
}