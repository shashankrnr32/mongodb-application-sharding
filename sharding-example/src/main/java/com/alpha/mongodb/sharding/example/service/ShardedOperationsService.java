package com.alpha.mongodb.sharding.example.service;

import com.alpha.mongodb.sharding.example.entity.TestShardedEntity;

import java.util.Optional;

public interface ShardedOperationsService {

    Optional<TestShardedEntity> findById(String id);

    Optional<TestShardedEntity> findByIndexedField(String indexedFieldValue);

    void insert(TestShardedEntity entity);
}
