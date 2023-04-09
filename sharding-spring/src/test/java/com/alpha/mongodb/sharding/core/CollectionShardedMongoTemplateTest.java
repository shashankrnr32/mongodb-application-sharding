package com.alpha.mongodb.sharding.core;

import com.alpha.mongodb.sharding.core.callback.HintResolutionCallback;
import com.alpha.mongodb.sharding.core.configuration.CollectionShardingOptions;
import com.alpha.mongodb.sharding.core.exception.UnresolvableCollectionShardException;
import com.alpha.mongodb.sharding.core.fixture.TestEntity1;
import com.alpha.mongodb.sharding.core.fixture.TestEntity3;
import com.alpha.mongodb.sharding.core.fixture.db.FindFromDatabaseIterable;
import com.alpha.mongodb.sharding.core.fixture.db.ListCollectionsFromDatabaseIterable;
import com.alpha.mongodb.sharding.core.hint.ShardingHintManager;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.DeleteOptions;
import lombok.Builder;
import lombok.Data;
import org.bson.Document;
import org.bson.types.ObjectId;
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
import org.springframework.data.mongodb.core.query.BasicUpdate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;

import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
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

    @Test
    public void testDeleteOne() {
        MongoTemplate mongoTemplate = getFixture(FixtureConfiguration.builder().registerHintResolutionCallback(true).build());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> MongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(mockMongoDatabase);

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);

        TestEntity3 testEntity3 = spy(new TestEntity3());
        testEntity3.setId(ObjectId.get().toString());
        mongoTemplate.remove(testEntity3);

        verify((HintResolutionCallback<TestEntity3>) collectionShardingOptions.getHintResolutionCallbacks().stream().findFirst().get())
                .resolveHintForDeleteContext(any(Document.class), eq(TestEntity3.class));
        verify(mockCollection).deleteOne(any(Document.class), any(DeleteOptions.class));
    }

    @Test
    public void testDeleteOneWhenShardHintManuallySet() {
        MongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> MongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(mockMongoDatabase);

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);

        TestEntity3 testEntity3 = spy(new TestEntity3());
        testEntity3.setId(ObjectId.get().toString());
        ShardingHintManager.setCollectionHint(String.valueOf(0));
        mongoTemplate.remove(testEntity3);

        verify(mockCollection).deleteOne(any(Document.class), any(DeleteOptions.class));
    }

    @Test(expected = UnresolvableCollectionShardException.class)
    public void testDeleteOneWhenShardHintNotSet() {
        MongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        TestEntity3 testEntity3 = spy(new TestEntity3());
        testEntity3.setId(ObjectId.get().toString());
        mongoTemplate.remove(testEntity3);
    }

    @Test(expected = UnresolvableCollectionShardException.class)
    public void testDeleteOneWhenCollectionShardHintNotSet() {
        MongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        TestEntity3 testEntity3 = spy(new TestEntity3());
        testEntity3.setId(ObjectId.get().toString());
        ShardingHintManager.setDatabaseHint(String.valueOf(0));
        mongoTemplate.remove(testEntity3);
    }

    @Test
    public void testGetCollection() {
        MongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> MongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(mockMongoDatabase);

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);

        ShardingHintManager.setCollectionHint(String.valueOf(0));
        MongoCollection<Document> mongoCollection = mongoTemplate.getCollection("TEST3");
        assertEquals(mockCollection, mongoCollection);
    }

    @Test
    public void testGetCollectionExists() {
        MongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> MongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(mockMongoDatabase);

        when(mockMongoDatabase.listCollectionNames()).thenReturn(
                new ListCollectionsFromDatabaseIterable(Collections.singletonList("TEST3_0")));

        ShardingHintManager.setCollectionHint(String.valueOf(0));
        assertTrue(mongoTemplate.collectionExists(TestEntity3.class));
    }

    @Test
    public void testGetCollectionExistsWhenPassingHint() {
        CollectionShardedMongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> MongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(mockMongoDatabase);

        when(mockMongoDatabase.listCollectionNames()).thenReturn(
                new ListCollectionsFromDatabaseIterable(Collections.singletonList("TEST3_0")));

        ShardingHintManager.setCollectionHint(String.valueOf(0));
        assertFalse(mongoTemplate.collectionExists("TEST3", String.valueOf(3)));
    }

    @Test
    public void testCreateCollection() {
        CollectionShardedMongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> MongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(mockMongoDatabase);

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);

        ShardingHintManager.setCollectionHint(String.valueOf(0));
        mongoTemplate.createCollection(TestEntity3.class);

        verify(mockMongoDatabase).createCollection(eq("TEST3_0"), any(CreateCollectionOptions.class));
    }

    @Test
    public void testFindById() {
        MongoTemplate mongoTemplate = getFixture(FixtureConfiguration.builder().registerHintResolutionCallback(true).build());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> MongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(mockMongoDatabase);

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);

        ObjectId objectId = ObjectId.get();
        Document documentFound = new Document();
        documentFound.put("_id", objectId);
        documentFound.put(TestEntity3.Fields.indexedField, "testIndexedFieldValue");

        when(mockCollection.find(any(Document.class), eq(Document.class)))
                .thenReturn(new FindFromDatabaseIterable(Collections.singletonList(documentFound)));

        TestEntity3 testEntity3 = mongoTemplate.findById(ObjectId.get(), TestEntity3.class);
        assertNotNull(testEntity3);
        assertEquals(objectId.toString(), testEntity3.getId());

        verify((HintResolutionCallback<TestEntity3>) collectionShardingOptions.getHintResolutionCallbacks().stream().findFirst().get())
                .resolveHintForFindContext(any(Document.class), eq(TestEntity3.class));
    }

    @Test
    public void testFindByIdWhenShardingHintManuallySet() {
        MongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> MongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(mockMongoDatabase);

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);

        ObjectId objectId = ObjectId.get();
        Document documentFound = new Document();
        documentFound.put("_id", objectId);
        documentFound.put(TestEntity3.Fields.indexedField, "testIndexedFieldValue");

        when(mockCollection.find(any(Document.class), eq(Document.class)))
                .thenReturn(new FindFromDatabaseIterable(Collections.singletonList(documentFound)));

        ShardingHintManager.setCollectionHint(String.valueOf(0));
        TestEntity3 testEntity3 = mongoTemplate.findById(ObjectId.get(), TestEntity3.class);
        assertNotNull(testEntity3);
        assertEquals(objectId.toString(), testEntity3.getId());
    }

    @Test
    public void testUpdate() {
        MongoTemplate mongoTemplate = getFixture(FixtureConfiguration.builder().registerHintResolutionCallback(true).build());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> MongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(mockMongoDatabase);

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);

        Query updateQuery = new Query();
        UpdateDefinition updateDefinition = new BasicUpdate(new Document()).addToSet("indexedField", "testIndexedValue");
        mongoTemplate.updateMulti(updateQuery, updateDefinition, TestEntity3.class);

        verify((HintResolutionCallback<TestEntity3>) collectionShardingOptions.getHintResolutionCallbacks().stream().findFirst().get())
                .resolveHintForUpdateContext(updateQuery, updateDefinition, TestEntity3.class);
    }

    @Test
    public void testUpdateWhenShardHintManuallySet() {
        MongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> MongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(mockMongoDatabase);

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);

        Query updateQuery = new Query();
        UpdateDefinition updateDefinition = new BasicUpdate(new Document()).addToSet("indexedField", "testIndexedValue");
        ShardingHintManager.setCollectionHint(String.valueOf(0));
        mongoTemplate.updateMulti(updateQuery, updateDefinition, TestEntity3.class);
    }

    @Test
    public void testFindAndRemove() {
        MongoTemplate mongoTemplate = getFixture(FixtureConfiguration.builder().registerHintResolutionCallback(true).build());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> MongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(mockMongoDatabase);

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);

        Query deleteQuery = new Query();
        mongoTemplate.findAndRemove(deleteQuery, TestEntity3.class);

        verify((HintResolutionCallback<TestEntity3>) collectionShardingOptions.getHintResolutionCallbacks().stream().findFirst().get())
                .resolveHintForDeleteContext(deleteQuery.getQueryObject(), TestEntity3.class);
    }

    @Test
    public void testFindAllAndRemove() {
        MongoTemplate mongoTemplate = getFixture(FixtureConfiguration.builder().registerHintResolutionCallback(true).build());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> MongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(mockMongoDatabase);

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);

        ObjectId objectId = ObjectId.get();
        Document documentFound = new Document();
        documentFound.put("_id", objectId);
        documentFound.put(TestEntity3.Fields.indexedField, "testIndexedFieldValue");
        when(mockCollection.find(any(Document.class), eq(Document.class)))
                .thenReturn(new FindFromDatabaseIterable(Collections.singletonList(documentFound)));
        when(mockCollection.withWriteConcern(any())).thenReturn(mockCollection);

        Query deleteQuery = new Query();
        mongoTemplate.findAllAndRemove(deleteQuery, TestEntity3.class);

        verify((HintResolutionCallback<TestEntity3>) collectionShardingOptions.getHintResolutionCallbacks().stream().findFirst().get())
                .resolveHintForDeleteContext(deleteQuery.getQueryObject(), TestEntity3.class);
    }

    @Test
    public void testFindAllAndRemoveWhenShardHintIsManuallySet() {
        MongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> MongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(mockMongoDatabase);

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);

        ObjectId objectId = ObjectId.get();
        Document documentFound = new Document();
        documentFound.put("_id", objectId);
        documentFound.put(TestEntity3.Fields.indexedField, "testIndexedFieldValue");
        when(mockCollection.find(any(Document.class), eq(Document.class)))
                .thenReturn(new FindFromDatabaseIterable(Collections.singletonList(documentFound)));
        when(mockCollection.withWriteConcern(any())).thenReturn(mockCollection);

        Query deleteQuery = new Query();
        ShardingHintManager.setCollectionHint(String.valueOf(0));
        mongoTemplate.findAllAndRemove(deleteQuery, TestEntity3.class);
    }


    @Test
    public void testFindAndRemoveWhenShardHintManuallySet() {
        MongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> MongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(mockMongoDatabase);

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);

        Query deleteQuery = new Query();
        ShardingHintManager.setCollectionHint(String.valueOf(0));
        mongoTemplate.findAndRemove(deleteQuery, TestEntity3.class);
    }

    @Test
    public void testFind() {
        MongoTemplate mongoTemplate = getFixture(FixtureConfiguration.builder().registerHintResolutionCallback(true).build());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> MongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(mockMongoDatabase);

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);

        ObjectId objectId = ObjectId.get();
        Document documentFound = new Document();
        documentFound.put("_id", objectId);
        documentFound.put(TestEntity3.Fields.indexedField, "testIndexedFieldValue");
        when(mockCollection.find(any(Document.class), eq(Document.class)))
                .thenReturn(new FindFromDatabaseIterable(Collections.singletonList(documentFound)));

        Query findQuery = new Query();
        ShardingHintManager.setCollectionHint(String.valueOf(0));
        List<TestEntity3> queryResult = mongoTemplate.find(findQuery, TestEntity3.class);

        assertEquals(1, queryResult.size());
        assertEquals(objectId.toString(), queryResult.get(0).getId());
        verify((HintResolutionCallback<TestEntity3>) collectionShardingOptions.getHintResolutionCallbacks().stream().findFirst().get())
                .resolveHintForFindContext(findQuery.getQueryObject(), TestEntity3.class);
    }

    @Test
    public void testFindWhenShardHintManuallySet() {
        MongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> MongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(mockMongoDatabase);

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);

        ObjectId objectId = ObjectId.get();
        Document documentFound = new Document();
        documentFound.put("_id", objectId);
        documentFound.put(TestEntity3.Fields.indexedField, "testIndexedFieldValue");
        when(mockCollection.find(any(Document.class), eq(Document.class)))
                .thenReturn(new FindFromDatabaseIterable(Collections.singletonList(documentFound)));

        Query findQuery = new Query();
        ShardingHintManager.setCollectionHint(String.valueOf(0));
        List<TestEntity3> queryResult = mongoTemplate.find(findQuery, TestEntity3.class);

        assertEquals(1, queryResult.size());
        assertEquals(objectId.toString(), queryResult.get(0).getId());
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