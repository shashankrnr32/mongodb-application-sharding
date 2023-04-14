package com.alpha.mongodb.sharding.example.configuration;

import com.alpha.mongodb.sharding.core.configuration.DatabaseShardingOptions;
import com.alpha.mongodb.sharding.core.template.DatabaseShardedReactiveMongoTemplate;
import com.mongodb.ConnectionString;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

@Configuration
@EnableReactiveMongoRepositories(
        basePackages = "com.alpha.mongodb.sharding.example.repository.database.reactive",
        reactiveMongoTemplateRef = "databaseShardedReactiveMongoTemplate")
public class DatabaseShardedReactiveMongoConfiguration {

    private static final String SPRING_MONGO_DB_URI_DATABASE_SHARDED_DS0 = "spring.mongodb.sharded.database.ds0.uri";
    private static final String SPRING_MONGO_DB_URI_DATABASE_SHARDED_DS1 = "spring.mongodb.sharded.database.ds1.uri";
    private static final String SPRING_MONGO_DB_URI_DATABASE_SHARDED_DS2 = "spring.mongodb.sharded.database.ds2.uri";

    @Autowired
    private Environment environment;

    @Bean
    public ReactiveMongoDatabaseFactory ds0ShardedReactiveMongoDbFactory() {
        return new SimpleReactiveMongoDatabaseFactory(new ConnectionString(environment.getProperty(SPRING_MONGO_DB_URI_DATABASE_SHARDED_DS0)));
    }

    @Bean
    public ReactiveMongoDatabaseFactory ds1ShardedReactiveMongoDbFactory() {
        return new SimpleReactiveMongoDatabaseFactory(new ConnectionString(environment.getProperty(SPRING_MONGO_DB_URI_DATABASE_SHARDED_DS1)));
    }

    @Bean
    public ReactiveMongoDatabaseFactory ds2ShardedReactiveMongoDbFactory() {
        return new SimpleReactiveMongoDatabaseFactory(new ConnectionString(environment.getProperty(SPRING_MONGO_DB_URI_DATABASE_SHARDED_DS2)));
    }

    @Bean("databaseShardedReactiveMongoTemplate")
    public ReactiveMongoTemplate databaseShardedReactiveMongoTemplate() {
        DatabaseShardingOptions shardingOptions = DatabaseShardingOptions.withIntegerStreamHints(IntStream.range(0, 3));
        Map<String, ReactiveMongoDatabaseFactory> factoryMap = new HashMap<>();
        factoryMap.put(String.valueOf(0), ds0ShardedReactiveMongoDbFactory());
        factoryMap.put(String.valueOf(1), ds1ShardedReactiveMongoDbFactory());
        factoryMap.put(String.valueOf(2), ds2ShardedReactiveMongoDbFactory());

        return new DatabaseShardedReactiveMongoTemplate(factoryMap, shardingOptions);
    }
}
