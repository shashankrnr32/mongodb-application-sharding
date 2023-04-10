package com.alpha.mongodb.sharding.core;

import com.alpha.mongodb.sharding.core.configuration.DatabaseShardingOptions;
import com.alpha.mongodb.sharding.core.exception.UnresolvableDatabaseShardException;
import com.alpha.mongodb.sharding.core.fixture.TestEntity3;
import com.alpha.mongodb.sharding.core.hint.ShardingHintManager;
import com.mongodb.client.MongoClient;
import lombok.Builder;
import lombok.Data;
import org.junit.After;
import org.junit.Test;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoExceptionTranslator;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.query.Query;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DatabaseShardedMongoTemplateTest {

    DatabaseShardingOptions databaseShardingOptions =
            DatabaseShardingOptions.withIntegerStreamHints(IntStream.range(0, 3));

    @Test
    public void testConstructorsWithMongoClientMap() {
        MongoClient mongoClient0 = mock(MongoClient.class);
        MongoClient mongoClient1 = mock(MongoClient.class);
        MongoClient mongoClient2 = mock(MongoClient.class);

        Map<String, MongoClient> mongoClientMap = new HashMap<>();

        mongoClientMap.put(String.valueOf(0), mongoClient0);
        mongoClientMap.put(String.valueOf(1), mongoClient1);
        mongoClientMap.put(String.valueOf(2), mongoClient2);

        DatabaseShardedMongoTemplate databaseShardedMongoTemplate =
                new DatabaseShardedMongoTemplate(mongoClientMap, "TEST_DB", databaseShardingOptions);
        assertNotNull(databaseShardedMongoTemplate);
        assertEquals(3, databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().size());
    }

    @Test
    public void testConstructorsWithMongoClient() {
        MongoClient mongoClient = mock(MongoClient.class);

        DatabaseShardedMongoTemplate databaseShardedMongoTemplate =
                new DatabaseShardedMongoTemplate(mongoClient, "TEST_DB", databaseShardingOptions);
        assertNotNull(databaseShardedMongoTemplate);
        assertEquals(3, databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().size());
    }

    @Test
    public void testConstructorWithMongoDatabaseFactoryMapAndConverter() {
        MongoDatabaseFactory databaseFactory0 = mock(MongoDatabaseFactory.class);
        databaseFactory0.getMongoDatabase("TEST_DB_0");
        when(databaseFactory0.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());

        MongoDatabaseFactory databaseFactory1 = mock(MongoDatabaseFactory.class);
        databaseFactory1.getMongoDatabase("TEST_DB_1");
        when(databaseFactory1.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());

        MongoDatabaseFactory databaseFactory2 = mock(MongoDatabaseFactory.class);
        databaseFactory2.getMongoDatabase("TEST_DB_2");
        when(databaseFactory2.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());

        Map<String, MongoDatabaseFactory> shardedDatabaseFactoryMap = new HashMap<>();
        shardedDatabaseFactoryMap.put(String.valueOf(0), databaseFactory0);
        shardedDatabaseFactoryMap.put(String.valueOf(1), databaseFactory1);
        shardedDatabaseFactoryMap.put(String.valueOf(2), databaseFactory2);

        DatabaseShardedMongoTemplate databaseShardedMongoTemplate =
                new DatabaseShardedMongoTemplate(shardedDatabaseFactoryMap, mock(MongoConverter.class), databaseShardingOptions);
        assertEquals(databaseShardingOptions, databaseShardedMongoTemplate.getShardingOptions());
        assertNotNull(databaseShardedMongoTemplate);
    }


    @Test
    public void testFind() {
        DatabaseShardedMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.builder().registerHintResolutionCallback(true).build());

        Query query = new Query();
        databaseShardedMongoTemplate.find(query, TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .find(query, TestEntity3.class);

        databaseShardedMongoTemplate.find(query, TestEntity3.class, "TEST3");
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .find(query, TestEntity3.class, "TEST3");

        databaseShardedMongoTemplate.findAll(TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .findAll(TestEntity3.class);

        databaseShardedMongoTemplate.findAll(TestEntity3.class, "TEST3");
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .findAll(TestEntity3.class, "TEST3");

        databaseShardedMongoTemplate.findDistinct(query, "indexedField", TestEntity3.class, TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .findDistinct(query, "indexedField", TestEntity3.class, TestEntity3.class);

        databaseShardedMongoTemplate.findDistinct(query, "indexedField", "TEST3", TestEntity3.class, TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .findDistinct(query, "indexedField", "TEST3", TestEntity3.class, TestEntity3.class);

        databaseShardedMongoTemplate.findDistinct("indexedField", TestEntity3.class, TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .findDistinct("indexedField", TestEntity3.class, TestEntity3.class);

        ShardingHintManager.setDatabaseHint(String.valueOf(0));
        databaseShardedMongoTemplate.findDistinct(query, "indexedField", "TEST3", TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .findDistinct(query, "indexedField", "TEST3", TestEntity3.class);

    }

    @Test
    public void testFindWhenShardHintManuallySet() {
        DatabaseShardedMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        Query query = new Query();
        ShardingHintManager.setDatabaseHint(String.valueOf(0));
        databaseShardedMongoTemplate.find(query, TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .find(query, TestEntity3.class);
    }

    @Test(expected = UnresolvableDatabaseShardException.class)
    public void testFindWhenShardHintNotSet() {
        DatabaseShardedMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        Query query = new Query();
        databaseShardedMongoTemplate.find(query, TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .find(query, TestEntity3.class);
    }

    @Test(expected = UnresolvableDatabaseShardException.class)
    public void testFindWhenDatabaseShardHintNotSet() {
        DatabaseShardedMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        Query query = new Query();
        ShardingHintManager.setCollectionHint(String.valueOf(0));
        databaseShardedMongoTemplate.find(query, TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .find(query, TestEntity3.class);
    }

    @After
    public void teardown() {
        ShardingHintManager.clear();
    }

    private DatabaseShardedMongoTemplate getFixture(FixtureConfiguration fixtureConfiguration) {
        if (fixtureConfiguration.isRegisterHintResolutionCallback()) {
            databaseShardingOptions.setHintResolutionCallbacks(Collections.singleton(
                    spy(new TestEntity3.TestEntity3DatabaseHintResolutionCallback())));
        }

        MongoDatabaseFactory databaseFactory0 = mock(MongoDatabaseFactory.class);
        databaseFactory0.getMongoDatabase("TEST_DB_0");
        when(databaseFactory0.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());

        MongoDatabaseFactory databaseFactory1 = mock(MongoDatabaseFactory.class);
        databaseFactory1.getMongoDatabase("TEST_DB_1");
        when(databaseFactory1.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());

        MongoDatabaseFactory databaseFactory2 = mock(MongoDatabaseFactory.class);
        databaseFactory2.getMongoDatabase("TEST_DB_2");
        when(databaseFactory2.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());

        Map<String, MongoDatabaseFactory> shardedDatabaseFactoryMap = new HashMap<>();
        shardedDatabaseFactoryMap.put(String.valueOf(0), databaseFactory0);
        shardedDatabaseFactoryMap.put(String.valueOf(1), databaseFactory1);
        shardedDatabaseFactoryMap.put(String.valueOf(2), databaseFactory2);

        DatabaseShardedMongoTemplate databaseShardedMongoTemplate =
                spy(new DatabaseShardedMongoTemplate(shardedDatabaseFactoryMap, databaseShardingOptions));
        assertEquals(databaseShardingOptions, databaseShardedMongoTemplate.getShardingOptions());
        assertNotNull(databaseShardedMongoTemplate);

        MongoTemplate mongoTemplate0 = mock(MongoTemplate.class);
        MongoTemplate mongoTemplate1 = mock(MongoTemplate.class);
        MongoTemplate mongoTemplate2 = mock(MongoTemplate.class);

        databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().put(String.valueOf(0), mongoTemplate0);
        databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().put(String.valueOf(1), mongoTemplate1);
        databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().put(String.valueOf(2), mongoTemplate2);

        return databaseShardedMongoTemplate;
    }

    @Data
    @Builder
    private static class FixtureConfiguration {
        private boolean registerHintResolutionCallback = false;

        private static FixtureConfiguration getDefault() {
            return builder().build();
        }
    }
}