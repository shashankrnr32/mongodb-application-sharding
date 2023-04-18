package com.alpha.mongodb.sharding.example.service;

import com.alpha.mongodb.sharding.example.api.models.EntityDTO;
import com.alpha.mongodb.sharding.example.entity.TestShardedEntity;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.Optional;

public interface ShardedOperationsService {

    Optional<EntityDTO> findById(String id);

    Optional<EntityDTO> findByIndexedField(String indexedFieldValue);

    EntityDTO insert(EntityDTO entity);

    void deleteById(String id);

    void deleteByIndexedField(String indexedField);

    default Query idQuery(String id) {
        return Query.query(new Criteria("_id").is(id));
    }

    default Query indexedFieldQuery(String indexedField) {
        return Query.query(new Criteria(TestShardedEntity.Fields.indexedField).is(indexedField));
    }
}
