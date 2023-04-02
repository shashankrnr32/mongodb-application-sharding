package com.alpha.mongodb.sharding.example.entity;

import com.alpha.mongodb.sharding.core.entity.CollectionShardedEntity;
import com.alpha.mongodb.sharding.core.entity.CompositeShardedEntity;
import com.alpha.mongodb.sharding.core.entity.DatabaseShardedEntity;
import lombok.Data;
import lombok.experimental.FieldNameConstants;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.FieldType;
import org.springframework.data.mongodb.core.mapping.MongoId;

@Document("TEST")
@Data
@FieldNameConstants
public class TestShardedEntity implements CollectionShardedEntity, DatabaseShardedEntity, CompositeShardedEntity {

    @MongoId(FieldType.OBJECT_ID)
    private String id;

    @Indexed(unique = true)
    private String indexedField;

    @Override
    public String resolveCollectionHint() {
        return String.valueOf(indexedField.charAt(1) - '0');
    }

    @Override
    public String resolveDatabaseHint() {
        return String.valueOf(indexedField.charAt(0) - '0');
    }
}
