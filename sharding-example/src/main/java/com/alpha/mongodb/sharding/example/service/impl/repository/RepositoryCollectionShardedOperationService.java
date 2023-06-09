package com.alpha.mongodb.sharding.example.service.impl.repository;

import com.alpha.mongodb.sharding.example.api.models.EntityDTO;
import com.alpha.mongodb.sharding.example.entity.TestShardedEntity;
import com.alpha.mongodb.sharding.example.repository.collection.executable.CollectionShardedEntityRepository;
import com.alpha.mongodb.sharding.example.service.ShardedOperationsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class RepositoryCollectionShardedOperationService implements ShardedOperationsService {

    @Autowired
    CollectionShardedEntityRepository collectionShardedEntityRepository;

    @Override
    public Optional<EntityDTO> findById(String id) {
        return collectionShardedEntityRepository.findById(id).map(TestShardedEntity::toDTO);
    }

    @Override
    public Optional<EntityDTO> findByIndexedField(String indexedFieldValue) {
        return collectionShardedEntityRepository.findByIndexedField(indexedFieldValue).map(TestShardedEntity::toDTO);
    }

    @Override
    public EntityDTO insert(EntityDTO entity) {
        return collectionShardedEntityRepository.insert(entity.toEntity()).toDTO();
    }

    @Override
    public void deleteByIndexedField(String indexedField) {
        collectionShardedEntityRepository.deleteByIndexedField(indexedField);
    }

    @Override
    public void deleteById(String id) {
        collectionShardedEntityRepository.deleteById(id);
    }
}
