package com.alpha.mongodb.sharding.example.service;

import com.alpha.mongodb.sharding.example.api.models.EntityDTO;

import java.util.Optional;

public interface ShardedOperationsService {

    Optional<EntityDTO> findById(String id);

    Optional<EntityDTO> findByIndexedField(String indexedFieldValue);

    void insert(EntityDTO entity);
}
