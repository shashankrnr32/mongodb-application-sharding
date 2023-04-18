package com.alpha.mongodb.sharding.example.service.impl.template;

import com.alpha.mongodb.sharding.example.api.models.EntityDTO;
import com.alpha.mongodb.sharding.example.entity.TestShardedEntity;
import com.alpha.mongodb.sharding.example.service.ShardedOperationsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class ReactiveTemplateDatabaseShardedOperationService implements ShardedOperationsService {

    @Autowired
    @Qualifier("databaseShardedReactiveMongoTemplate")
    ReactiveMongoTemplate databaseShardedEntityReactiveMongoTemplate;

    @Override
    public Optional<EntityDTO> findById(String id) {
        return Optional.ofNullable(databaseShardedEntityReactiveMongoTemplate.findById(id, TestShardedEntity.class).map(TestShardedEntity::toDTO).block());
    }

    @Override
    public Optional<EntityDTO> findByIndexedField(String indexedFieldValue) {
        Criteria criteria = Criteria.where(TestShardedEntity.Fields.indexedField).is(indexedFieldValue);
        Query query = new Query(criteria);
        return Optional.ofNullable(databaseShardedEntityReactiveMongoTemplate.findOne(query, TestShardedEntity.class).map(TestShardedEntity::toDTO).block());
    }

    @Override
    public EntityDTO insert(EntityDTO entity) {
        return databaseShardedEntityReactiveMongoTemplate.insert(entity.toEntity()).map(TestShardedEntity::toDTO).block();
    }

    @Override
    public void deleteByIndexedField(String indexedField) {
        databaseShardedEntityReactiveMongoTemplate.remove(indexedFieldQuery(indexedField), TestShardedEntity.class).block();
    }

    @Override
    public void deleteById(String id) {
        databaseShardedEntityReactiveMongoTemplate.remove(idQuery(id)).block();
    }
}
