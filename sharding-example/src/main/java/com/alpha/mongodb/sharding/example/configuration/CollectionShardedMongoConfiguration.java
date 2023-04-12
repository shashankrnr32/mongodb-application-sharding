package com.alpha.mongodb.sharding.example.configuration;

import com.alpha.mongodb.sharding.core.configuration.CollectionShardingOptions;
import com.alpha.mongodb.sharding.core.template.CollectionShardedMongoTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

import java.util.stream.IntStream;

@Configuration
@EnableMongoRepositories(
        basePackages = "com.alpha.mongodb.sharding.example.repository.collection",
        mongoTemplateRef = "collectionShardedMongoTemplate")
public class CollectionShardedMongoConfiguration {

    private static final String SPRING_MONGO_DB_URI_COLLECTION_SHARDED = "spring.mongodb.sharded.collection.uri";

    @Autowired
    private Environment environment;

    @Bean
    @Primary
    public MongoDatabaseFactory collectionShardedMongoDbFactory() {
        return new SimpleMongoClientDatabaseFactory(environment.getProperty(SPRING_MONGO_DB_URI_COLLECTION_SHARDED));
    }

    @Bean("collectionShardedMongoTemplate")
    @Primary
    public MongoTemplate collectionShardedMongoTemplate() {
        CollectionShardingOptions shardingOptions =
                CollectionShardingOptions.withIntegerStreamHints(IntStream.range(0, 3));

        return new CollectionShardedMongoTemplate(
                collectionShardedMongoDbFactory(), shardingOptions);
    }


}
