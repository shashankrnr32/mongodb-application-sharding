package com.alpha.mongodb.sharding.example.service.impl.repository;

import com.alpha.mongodb.sharding.example.api.models.EntityDTO;
import com.alpha.mongodb.sharding.example.entity.TestShardedEntity;
import com.alpha.mongodb.sharding.example.repository.collection.reactive.CollectionShardedEntityReactiveRepository;
import com.alpha.mongodb.sharding.example.service.ShardedOperationsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class ReactiveRepositoryCollectionShardedOperationService implements ShardedOperationsService {

    @Autowired
    CollectionShardedEntityReactiveRepository collectionShardedEntityReactiveRepository;

    @Override
    public Optional<EntityDTO> findById(String id) {
        return Optional.ofNullable(collectionShardedEntityReactiveRepository
                .findById(id).map(TestShardedEntity::toDTO).block());
    }

    @Override
    public Optional<EntityDTO> findByIndexedField(String indexedFieldValue) {
        return collectionShardedEntityReactiveRepository
                .findByIndexedField(indexedFieldValue)
                .map(entityOptional -> entityOptional.map(TestShardedEntity::toDTO)).block();
    }

    @Override
    public EntityDTO insert(EntityDTO entity) {
        return collectionShardedEntityReactiveRepository.insert(entity.toEntity()).block().toDTO();
    }
}
