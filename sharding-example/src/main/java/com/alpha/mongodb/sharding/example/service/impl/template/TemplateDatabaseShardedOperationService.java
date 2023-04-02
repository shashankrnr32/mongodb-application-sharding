package com.alpha.mongodb.sharding.example.service.impl.template;

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
public class TemplateDatabaseShardedOperationService implements ShardedOperationsService {

    @Autowired
    @Qualifier("databaseShardedMongoTemplate")
    MongoTemplate databaseShardedEntityMongoTemplate;

    @Override
    public Optional<TestShardedEntity> findById(String id) {
        return Optional.ofNullable(databaseShardedEntityMongoTemplate.findById(id, TestShardedEntity.class));
    }

    @Override
    public Optional<TestShardedEntity> findByIndexedField(String indexedFieldValue) {
        Criteria criteria = Criteria.where(TestShardedEntity.Fields.indexedField).is(indexedFieldValue);
        Query query = new Query(criteria);
        return Optional.ofNullable(databaseShardedEntityMongoTemplate.findOne(query, TestShardedEntity.class));
    }

    @Override
    public void insert(TestShardedEntity entity) {
        databaseShardedEntityMongoTemplate.insert(entity);
    }
}
