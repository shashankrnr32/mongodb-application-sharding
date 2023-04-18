package com.alpha.mongodb.sharding.example.repository.database.executable;

import com.alpha.mongodb.sharding.example.entity.TestShardedEntity;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface DatabaseShardedEntityRepository extends MongoRepository<TestShardedEntity, String> {

    Optional<TestShardedEntity> findOneByIndexedField(String indexedField);

    void deleteByIndexedField(String indexedField);

}
