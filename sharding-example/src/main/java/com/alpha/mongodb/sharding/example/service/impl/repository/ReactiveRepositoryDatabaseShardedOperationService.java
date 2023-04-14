package com.alpha.mongodb.sharding.example.service.impl.repository;

import com.alpha.mongodb.sharding.example.api.models.EntityDTO;
import com.alpha.mongodb.sharding.example.entity.TestShardedEntity;
import com.alpha.mongodb.sharding.example.repository.database.reactive.DatabaseShardedEntityReactiveRepository;
import com.alpha.mongodb.sharding.example.service.ShardedOperationsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class ReactiveRepositoryDatabaseShardedOperationService implements ShardedOperationsService {

    @Autowired
    DatabaseShardedEntityReactiveRepository databaseShardedEntityReactiveRepository;

    @Override
    public Optional<EntityDTO> findById(String id) {
        return Optional.ofNullable(databaseShardedEntityReactiveRepository
                .findById(id).map(TestShardedEntity::toDTO).block());
    }

    @Override
    public Optional<EntityDTO> findByIndexedField(String indexedFieldValue) {
        return databaseShardedEntityReactiveRepository
                .findByIndexedField(indexedFieldValue)
                .map(entityOptional -> entityOptional.map(TestShardedEntity::toDTO)).block();
    }

    @Override
    public void insert(EntityDTO entity) {
        databaseShardedEntityReactiveRepository.insert(entity.toEntity()).block();
    }
}