package com.alpha.mongodb.sharding.core;

import com.alpha.mongodb.sharding.core.callback.HintResolutionCallback;
import com.alpha.mongodb.sharding.core.configuration.CollectionShardingOptions;
import com.alpha.mongodb.sharding.core.exception.UnresolvableCollectionShardException;
import com.alpha.mongodb.sharding.core.fixtures.TestEntity1;
import com.alpha.mongodb.sharding.core.fixtures.TestEntity3;
import com.alpha.mongodb.sharding.core.hint.ShardingHintManager;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.Builder;
import lombok.Data;
import org.bson.Document;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoDatabaseUtils;
import org.springframework.data.mongodb.core.MongoExceptionTranslator;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoConverter;

import java.util.Collections;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CollectionShardedMongoTemplateTest {

    CollectionShardingOptions collectionShardingOptions =
            CollectionShardingOptions.withIntegerStreamHints(IntStream.range(0, 3));

    MockedStatic<MongoDatabaseUtils> mongoDatabaseUtilsMockedStatic = mockStatic(MongoDatabaseUtils.class);

    @Test
    public void testConstructorWithMongoClient() {
        MongoClient mongoClient = mock(MongoClient.class);
        CollectionShardedMongoTemplate collectionShardedMongoTemplate = new CollectionShardedMongoTemplate(
                mongoClient, "TEST_DATABASE", collectionShardingOptions);
        assertEquals(collectionShardingOptions, collectionShardedMongoTemplate.getShardingOptions());
        assertNotNull(collectionShardedMongoTemplate);
    }

    @Test
    public void testConstructorWithDatabaseFactory() {
        MongoDatabaseFactory databaseFactory = mock(MongoDatabaseFactory.class);
        when(databaseFactory.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());

        CollectionShardedMongoTemplate collectionShardedMongoTemplate =
                new CollectionShardedMongoTemplate(databaseFactory, collectionShardingOptions);
        assertEquals(collectionShardingOptions, collectionShardedMongoTemplate.getShardingOptions());
        assertNotNull(collectionShardedMongoTemplate);
    }

    @Test
    public void testConstructorWithDatabaseFactoryAndConverter() {
        MongoDatabaseFactory databaseFactory = mock(MongoDatabaseFactory.class);
        when(databaseFactory.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());

        CollectionShardedMongoTemplate collectionShardedMongoTemplate =
                new CollectionShardedMongoTemplate(databaseFactory, mock(MongoConverter.class), collectionShardingOptions);
        assertEquals(collectionShardingOptions, collectionShardedMongoTemplate.getShardingOptions());
        assertNotNull(collectionShardedMongoTemplate);
    }

    @Test
    public void testInsert() {
        MongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> MongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(mockMongoDatabase);

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);

        TestEntity3 testEntity3 = spy(new TestEntity3());
        TestEntity3 persistedTestEntity3 = mongoTemplate.insert(testEntity3);

        verify(testEntity3, times(1)).resolveCollectionHint();
        verify(mockCollection).insertOne(any(Document.class));
    }

    @Test
    public void testInsertWithShardingHintManuallySet() {
        MongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> MongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(mockMongoDatabase);

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST1_0", Document.class))
                .thenReturn(mockCollection);

        TestEntity1 testEntity1 = spy(new TestEntity1());
        ShardingHintManager.setCollectionHint(String.valueOf(0));
        TestEntity1 persistedTestEntity3 = mongoTemplate.insert(testEntity1);

        verify(mockCollection).insertOne(any(Document.class));
    }

    @Test(expected = UnresolvableCollectionShardException.class)
    public void testInsertWithShardingHintNotManuallySet() {
        MongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        TestEntity1 testEntity1 = spy(new TestEntity1());
        TestEntity1 persistedTestEntity3 = mongoTemplate.insert(testEntity1);
    }

    @Test(expected = UnresolvableCollectionShardException.class)
    public void testInsertWithCollectionShardingHintNotManuallySet() {
        MongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        TestEntity1 testEntity1 = spy(new TestEntity1());
        ShardingHintManager.setDatabaseHint(String.valueOf(0));
        TestEntity1 persistedTestEntity3 = mongoTemplate.insert(testEntity1);
    }

    @Test(expected = UnresolvableCollectionShardException.class)
    public void testInsertWithCollectionShardingHintNotPassingValidation() {
        MongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        TestEntity1 testEntity1 = spy(new TestEntity1());
        ShardingHintManager.setCollectionHint(String.valueOf(4));
        TestEntity1 persistedTestEntity3 = mongoTemplate.insert(testEntity1);
    }

    @Test
    public void testInsertWithHintResolutionCallback() {
        MongoTemplate mongoTemplate = getFixture(FixtureConfiguration
                .builder()
                .registerHintResolutionCallback(true)
                .build());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> MongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(mockMongoDatabase);

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);

        TestEntity3 testEntity3 = spy(new TestEntity3());
        TestEntity3 persistedTestEntity3 = mongoTemplate.insert(testEntity3);

        verify((HintResolutionCallback<TestEntity3>) collectionShardingOptions.getHintResolutionCallbacks().stream().findFirst().get())
                .resolveHintForSaveContext(testEntity3);
        verify(testEntity3, times(1)).resolveCollectionHint();
        verify(mockCollection).insertOne(any(Document.class));
    }

    @Test
    public void testSave() {
        MongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> MongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(mockMongoDatabase);

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);

        TestEntity3 testEntity3 = spy(new TestEntity3());
        TestEntity3 persistedTestEntity3 = mongoTemplate.save(testEntity3);

        verify(testEntity3, times(1)).resolveCollectionHint();
        verify(mockCollection).insertOne(any(Document.class));
    }

    @After
    public void teardown() {
        ShardingHintManager.clear();
        mongoDatabaseUtilsMockedStatic.close();
    }

    private CollectionShardedMongoTemplate getFixture(FixtureConfiguration fixtureConfiguration) {
        if (fixtureConfiguration.isRegisterHintResolutionCallback()) {
            collectionShardingOptions.setHintResolutionCallbacks(Collections.singleton(
                    spy(new TestEntity3.TestEntity3CollectionHintResolutionCallback())));
        }

        MongoDatabaseFactory databaseFactory = mock(MongoDatabaseFactory.class);
        databaseFactory.getMongoDatabase("TEST_DB");
        when(databaseFactory.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());

        CollectionShardedMongoTemplate collectionShardedMongoTemplate =
                spy(new CollectionShardedMongoTemplate(databaseFactory, collectionShardingOptions));
        assertEquals(collectionShardingOptions, collectionShardedMongoTemplate.getShardingOptions());
        assertNotNull(collectionShardedMongoTemplate);

        return collectionShardedMongoTemplate;
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