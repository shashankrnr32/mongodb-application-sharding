package com.alpha.mongodb.sharding.example.repository.collection;

import com.alpha.mongodb.sharding.example.entity.TestShardedEntity;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface CollectionShardedEntityRepository extends MongoRepository<TestShardedEntity, String> {

    Optional<TestShardedEntity> findByIndexedField(String indexedField);

}
