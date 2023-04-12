package com.alpha.mongodb.sharding.core.template;

import com.alpha.mongodb.sharding.core.configuration.CompositeShardingOptions;
import com.mongodb.client.MongoClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoExceptionTranslator;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CompositeShardedMongoTemplateTest {

    private CompositeShardedMongoTemplate compositeShardedMongoTemplate;

    @Mock
    private MongoClient mongoClient;

    @Mock
    private MongoDatabaseFactory mongoDatabaseFactory;

    @Mock
    private MappingMongoConverter mappingMongoConverter;

    private CompositeShardingOptions compositeShardingOptions;

    @Before
    public void setup() {
        compositeShardingOptions = CompositeShardingOptions.withIntegerStreamHints(IntStream.range(0, 3), IntStream.range(0, 3));
    }

    @Test
    public void testConstructorWithMongoClient() {
        compositeShardedMongoTemplate = new CompositeShardedMongoTemplate(mongoClient, "testDB", compositeShardingOptions);
    }

    @Test
    public void testConstructorWithDelegatedDatabaseFactory() {
        when(mongoDatabaseFactory.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());
        Map<String, MongoDatabaseFactory> delegatedDatabaseFactory = new HashMap<>();
        delegatedDatabaseFactory.put(String.valueOf(0), mongoDatabaseFactory);
        delegatedDatabaseFactory.put(String.valueOf(1), mongoDatabaseFactory);
        delegatedDatabaseFactory.put(String.valueOf(2), mongoDatabaseFactory);
        compositeShardedMongoTemplate = new CompositeShardedMongoTemplate(delegatedDatabaseFactory, compositeShardingOptions);
    }

    @Test
    public void testConstructorWithDelegatedDatabaseFactoryAndMongoConverter() {
        Map<String, MongoDatabaseFactory> delegatedDatabaseFactory = new HashMap<>();
        delegatedDatabaseFactory.put(String.valueOf(0), mongoDatabaseFactory);
        delegatedDatabaseFactory.put(String.valueOf(1), mongoDatabaseFactory);
        delegatedDatabaseFactory.put(String.valueOf(2), mongoDatabaseFactory);
        compositeShardedMongoTemplate = new CompositeShardedMongoTemplate(delegatedDatabaseFactory, mappingMongoConverter, compositeShardingOptions);
    }

}
