package com.alpha.mongodb.sharding.example.repository.collection.executable;

import com.alpha.mongodb.sharding.example.entity.TestShardedEntity;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface CollectionShardedEntityRepository extends MongoRepository<TestShardedEntity, String> {

    Optional<TestShardedEntity> findByIndexedField(String indexedField);

    void deleteByIndexedField(String indexedField);

}
