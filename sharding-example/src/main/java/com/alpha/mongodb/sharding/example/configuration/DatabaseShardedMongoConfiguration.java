package com.alpha.mongodb.sharding.example.configuration;

import com.alpha.mongodb.sharding.core.DatabaseShardedMongoTemplate;
import com.alpha.mongodb.sharding.core.configuration.DatabaseShardingOptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

@Configuration
public class DatabaseShardedMongoConfiguration {

    private static final String SPRING_MONGO_DB_URI_DATABASE_SHARDED_DS0 = "spring.mongodb.sharded.database.ds0.uri";
    private static final String SPRING_MONGO_DB_URI_DATABASE_SHARDED_DS1 = "spring.mongodb.sharded.database.ds1.uri";
    private static final String SPRING_MONGO_DB_URI_DATABASE_SHARDED_DS2 = "spring.mongodb.sharded.database.ds2.uri";

    @Autowired
    private Environment environment;

    @Bean
    public MongoDatabaseFactory ds0ShardedMongoDbFactory() {
        return new SimpleMongoClientDatabaseFactory(environment.getProperty(SPRING_MONGO_DB_URI_DATABASE_SHARDED_DS0));
    }

    @Bean
    public MongoDatabaseFactory ds1ShardedMongoDbFactory() {
        return new SimpleMongoClientDatabaseFactory(environment.getProperty(SPRING_MONGO_DB_URI_DATABASE_SHARDED_DS1));
    }

    @Bean
    public MongoDatabaseFactory ds2ShardedMongoDbFactory() {
        return new SimpleMongoClientDatabaseFactory(environment.getProperty(SPRING_MONGO_DB_URI_DATABASE_SHARDED_DS2));
    }

    @Bean("databaseShardedMongoTemplate")
    public MongoTemplate databaseShardedMongoTemplate() {
        DatabaseShardingOptions shardingOptions = DatabaseShardingOptions.withIntegerStreamHints(IntStream.range(0, 3));
        Map<String, MongoDatabaseFactory> factoryMap = new HashMap<String, MongoDatabaseFactory>() {{
            put(String.valueOf(0), ds0ShardedMongoDbFactory());
            put(String.valueOf(1), ds1ShardedMongoDbFactory());
            put(String.valueOf(2), ds2ShardedMongoDbFactory());
        }};
        return new DatabaseShardedMongoTemplate(factoryMap, shardingOptions);
    }
}
