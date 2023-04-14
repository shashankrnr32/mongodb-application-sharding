package com.alpha.mongodb.sharding.example.service.impl.repository;

import com.alpha.mongodb.sharding.example.api.models.EntityDTO;
import com.alpha.mongodb.sharding.example.entity.TestShardedEntity;
import com.alpha.mongodb.sharding.example.repository.database.executable.DatabaseShardedEntityRepository;
import com.alpha.mongodb.sharding.example.service.ShardedOperationsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class RepositoryDatabaseShardedOperationService implements ShardedOperationsService {

    @Autowired
    DatabaseShardedEntityRepository databaseShardedEntityRepository;

    @Override
    public Optional<EntityDTO> findById(String id) {
        return databaseShardedEntityRepository.findById(id).map(TestShardedEntity::toDTO);
    }

    @Override
    public Optional<EntityDTO> findByIndexedField(String indexedFieldValue) {
        return databaseShardedEntityRepository.findByIndexedField(indexedFieldValue).map(TestShardedEntity::toDTO);
    }

    @Override
    public void insert(EntityDTO entity) {
        databaseShardedEntityRepository.insert(entity.toEntity());
    }
}
