package com.alpha.mongodb.sharding.example.configuration;

import com.alpha.mongodb.sharding.core.CollectionShardedReactiveMongoTemplate;
import com.alpha.mongodb.sharding.core.configuration.CollectionShardingOptions;
import com.mongodb.ConnectionString;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories;

import java.util.stream.IntStream;

@Configuration
@EnableReactiveMongoRepositories(
        basePackages = "com.alpha.mongodb.sharding.example.repository.collection.reactive",
        reactiveMongoTemplateRef = "collectionShardedReactiveMongoTemplate")
public class CollectionShardedReactiveMongoConfiguration {
    private static final String SPRING_MONGO_DB_URI_COLLECTION_SHARDED = "spring.mongodb.sharded.collection.uri";

    @Autowired
    private Environment environment;

    @Bean
    @Primary
    public ReactiveMongoDatabaseFactory collectionShardedReactiveMongoDbFactory() {
        return new SimpleReactiveMongoDatabaseFactory(
                new ConnectionString(environment.getProperty(SPRING_MONGO_DB_URI_COLLECTION_SHARDED)));
    }

    @Bean("collectionShardedReactiveMongoTemplate")
    @Primary
    public ReactiveMongoTemplate collectionShardedReactiveMongoTemplate() {
        CollectionShardingOptions shardingOptions =
                CollectionShardingOptions.withIntegerStreamHints(IntStream.range(0, 3));

        return new CollectionShardedReactiveMongoTemplate(
                collectionShardedReactiveMongoDbFactory(), shardingOptions);
    }


}
