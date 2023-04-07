package com.alpha.mongodb.sharding.example.service.impl.template;

import com.alpha.mongodb.sharding.example.api.models.EntityDTO;
import com.alpha.mongodb.sharding.example.entity.TestShardedEntity;
import com.alpha.mongodb.sharding.example.service.ShardedOperationsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class TemplateCollectionShardedOperationService implements ShardedOperationsService {

    @Autowired
    @Qualifier("collectionShardedMongoTemplate")
    MongoTemplate collectionShardedEntityMongoTemplate;

    @Override
    public Optional<EntityDTO> findById(String id) {
        return Optional.ofNullable(collectionShardedEntityMongoTemplate.findById(id, TestShardedEntity.class)).map(TestShardedEntity::toDTO);
    }

    @Override
    public Optional<EntityDTO> findByIndexedField(String indexedFieldValue) {
        Criteria criteria = Criteria.where(TestShardedEntity.Fields.indexedField).is(indexedFieldValue);
        Query query = new Query(criteria);
        return Optional.ofNullable(collectionShardedEntityMongoTemplate.findOne(query, TestShardedEntity.class)).map(TestShardedEntity::toDTO);
    }

    @Override
    public void insert(EntityDTO entity) {
        collectionShardedEntityMongoTemplate.insert(entity.toEntity());
    }
}
