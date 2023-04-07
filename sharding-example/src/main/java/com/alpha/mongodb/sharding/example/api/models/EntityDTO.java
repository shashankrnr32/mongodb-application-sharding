package com.alpha.mongodb.sharding.example.api.models;

import com.alpha.mongodb.sharding.example.entity.TestShardedEntity;
import lombok.Data;

@Data
public class EntityDTO {
    private String indexedField;

    public TestShardedEntity toEntity() {
        TestShardedEntity entity = new TestShardedEntity();
        entity.setIndexedField(indexedField);
        return entity;
    }
}
