package com.alpha.mongodb.sharding.example.repository.collection.reactive;

import com.alpha.mongodb.sharding.example.entity.TestShardedEntity;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.util.Optional;

@Repository
public interface CollectionShardedEntityReactiveRepository extends ReactiveMongoRepository<TestShardedEntity, String> {

    Mono<Optional<TestShardedEntity>> findByIndexedField(String indexedField);

}