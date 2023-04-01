package com.alpha.mongodb.sharding.example.service.impl.repository;

import com.alpha.mongodb.sharding.example.entity.TestShardedEntity;
import com.alpha.mongodb.sharding.example.repository.collection.CollectionShardedEntityRepository;
import com.alpha.mongodb.sharding.example.service.ShardedOperationsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class RepositoryCollectionShardedOperationService implements ShardedOperationsService {

    @Autowired
    CollectionShardedEntityRepository collectionShardedEntityRepository;

    @Override
    public Optional<TestShardedEntity> findById(String id) {
        return collectionShardedEntityRepository.findById(id);
    }

    @Override
    public Optional<TestShardedEntity> findByIndexedField(String indexedFieldValue) {
        return collectionShardedEntityRepository.findByIndexedField(indexedFieldValue);
    }

    @Override
    public void insert(TestShardedEntity entity) {
        collectionShardedEntityRepository.insert(entity);
    }
}
