package com.alpha.mongodb.sharding.core.template;

import com.alpha.mongodb.sharding.core.callback.HintResolutionCallback;
import com.alpha.mongodb.sharding.core.configuration.CollectionShardingOptions;
import com.alpha.mongodb.sharding.core.exception.UnresolvableCollectionShardException;
import com.alpha.mongodb.sharding.core.fixture.TestEntity1;
import com.alpha.mongodb.sharding.core.fixture.TestEntity3;
import com.alpha.mongodb.sharding.core.fixture.db.FindFromDatabasePublisher;
import com.alpha.mongodb.sharding.core.fixture.db.ListCollectionsFromDatabaseIterable;
import com.alpha.mongodb.sharding.core.hint.ShardingHintManager;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import lombok.Builder;
import lombok.Data;
import org.bson.BsonObjectId;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.ReactiveMongoDatabaseUtils;
import org.springframework.data.mongodb.core.MongoExceptionTranslator;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.query.BasicUpdate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CollectionShardedReactiveMongoTemplateTest {

    CollectionShardingOptions collectionShardingOptions =
            CollectionShardingOptions.withIntegerStreamHints(IntStream.range(0, 3));

    MockedStatic<ReactiveMongoDatabaseUtils> mongoDatabaseUtilsMockedStatic = mockStatic(ReactiveMongoDatabaseUtils.class);

    @Test
    public void testConstructorWithMongoClient() {
        MongoClient mongoClient = mock(MongoClient.class);
        CollectionShardedReactiveMongoTemplate collectionShardedMongoTemplate = new CollectionShardedReactiveMongoTemplate(
                mongoClient, "TEST_DATABASE", collectionShardingOptions);
        assertEquals(collectionShardingOptions, collectionShardedMongoTemplate.getShardingOptions());
        assertNotNull(collectionShardedMongoTemplate);
    }

    @Test
    public void testConstructorWithDatabaseFactory() {
        ReactiveMongoDatabaseFactory databaseFactory = mock(ReactiveMongoDatabaseFactory.class);
        when(databaseFactory.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());

        CollectionShardedReactiveMongoTemplate collectionShardedMongoTemplate =
                new CollectionShardedReactiveMongoTemplate(databaseFactory, collectionShardingOptions);
        assertEquals(collectionShardingOptions, collectionShardedMongoTemplate.getShardingOptions());
        assertNotNull(collectionShardedMongoTemplate);
    }

    @Test
    public void testConstructorWithDatabaseFactoryAndConverter() {
        ReactiveMongoDatabaseFactory databaseFactory = mock(ReactiveMongoDatabaseFactory.class);
        when(databaseFactory.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());

        CollectionShardedReactiveMongoTemplate collectionShardedMongoTemplate =
                new CollectionShardedReactiveMongoTemplate(databaseFactory, mock(MongoConverter.class), collectionShardingOptions);
        assertEquals(collectionShardingOptions, collectionShardedMongoTemplate.getShardingOptions());
        assertNotNull(collectionShardedMongoTemplate);
    }

    @Test
    public void testInsert() {
        CollectionShardedReactiveMongoTemplate mongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> ReactiveMongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(Mono.just(mockMongoDatabase));

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);
        when(mockCollection.insertOne(any(Document.class)))
                .thenAnswer(invocation -> {
                    ObjectId objectId = ObjectId.get();
                    ((Document) invocation.getArgument(0)).put("_id", objectId);
                    return Flux.fromIterable(Collections.singletonList(
                            InsertOneResult.acknowledged(new BsonObjectId(objectId))));
                });

        TestEntity3 testEntity3 = spy(new TestEntity3());

        mongoTemplate.insert(testEntity3).block();
        verify(testEntity3, times(1)).resolveCollectionHint();
    }

    @Test
    public void testInsertWithShardingHintManuallySet() {
        CollectionShardedReactiveMongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> ReactiveMongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(Mono.just(mockMongoDatabase));

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST1_0", Document.class))
                .thenReturn(mockCollection);
        when(mockCollection.insertOne(any(Document.class)))
                .thenAnswer(invocation -> {
                    ObjectId objectId = ObjectId.get();
                    ((Document) invocation.getArgument(0)).put("_id", objectId);
                    return Flux.fromIterable(Collections.singletonList(
                            InsertOneResult.acknowledged(new BsonObjectId(objectId))));
                });

        TestEntity1 testEntity1 = spy(new TestEntity1());
        ShardingHintManager.setCollectionHint(String.valueOf(0));
        mongoTemplate.insert(testEntity1).block();

        verify(mockCollection).insertOne(any(Document.class));
    }

    @Test(expected = UnresolvableCollectionShardException.class)
    public void testInsertWithShardingHintNotManuallySet() {
        CollectionShardedReactiveMongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        TestEntity1 testEntity1 = spy(new TestEntity1());
        mongoTemplate.insert(testEntity1);
    }

    @Test(expected = UnresolvableCollectionShardException.class)
    public void testInsertWithCollectionShardingHintNotManuallySet() {
        CollectionShardedReactiveMongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        TestEntity1 testEntity1 = spy(new TestEntity1());
        ShardingHintManager.setDatabaseHint(String.valueOf(0));
        mongoTemplate.insert(testEntity1);
    }

    @Test(expected = UnresolvableCollectionShardException.class)
    public void testInsertWithCollectionShardingHintNotPassingValidation() {
        CollectionShardedReactiveMongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        TestEntity1 testEntity1 = spy(new TestEntity1());
        ShardingHintManager.setCollectionHint(String.valueOf(4));
        mongoTemplate.insert(testEntity1);
    }

    @Test
    public void testInsertWithHintResolutionCallback() {
        CollectionShardedReactiveMongoTemplate mongoTemplate = getFixture(FixtureConfiguration
                .builder()
                .registerHintResolutionCallback(true)
                .build());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> ReactiveMongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(Mono.just(mockMongoDatabase));

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);
        when(mockCollection.insertOne(any(Document.class)))
                .thenAnswer(invocation -> {
                    ObjectId objectId = ObjectId.get();
                    ((Document) invocation.getArgument(0)).put("_id", objectId);
                    return Flux.fromIterable(Collections.singletonList(
                            InsertOneResult.acknowledged(new BsonObjectId(objectId))));
                });

        TestEntity3 testEntity3 = spy(new TestEntity3());
        mongoTemplate.insert(testEntity3).block();

        verify((HintResolutionCallback<TestEntity3>) collectionShardingOptions.getHintResolutionCallbacks().stream().findFirst().get())
                .resolveHintForSaveContext(testEntity3);
        verify(testEntity3, times(1)).resolveCollectionHint();
        verify(mockCollection).insertOne(any(Document.class));
    }

    @Test
    public void testSave() {
        CollectionShardedReactiveMongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> ReactiveMongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(Mono.just(mockMongoDatabase));

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);
        when(mockCollection.insertOne(any(Document.class))).thenAnswer(invocationOnMock -> {
            ObjectId objectId = ObjectId.get();
            ((Document) invocationOnMock.getArgument(0)).put("_id", objectId);
            return Flux.fromIterable(Collections.singletonList(
                    InsertOneResult.acknowledged(new BsonObjectId(objectId))));
        });

        TestEntity3 testEntity3 = spy(new TestEntity3());
        mongoTemplate.save(testEntity3).block();

        verify(testEntity3, times(1)).resolveCollectionHint();
    }

    @Test
    public void testDeleteOne() {
        CollectionShardedReactiveMongoTemplate mongoTemplate = getFixture(FixtureConfiguration.builder().registerHintResolutionCallback(true).build());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> ReactiveMongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(Mono.just(mockMongoDatabase));

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);

        ObjectId objectId = ObjectId.get();
        Document documentFound = new Document();
        documentFound.put("_id", objectId);
        documentFound.put(TestEntity3.Fields.indexedField, "testIndexedFieldValue");
        when(mockCollection.deleteMany(any(Document.class), any(DeleteOptions.class))).thenReturn(Flux.empty());

        TestEntity3 testEntity3 = spy(new TestEntity3());
        testEntity3.setId(ObjectId.get().toString());
        mongoTemplate.remove(testEntity3).block();

        verify((HintResolutionCallback<TestEntity3>) collectionShardingOptions.getHintResolutionCallbacks().stream().findFirst().get())
                .resolveHintForDeleteContext(any(Document.class), eq(TestEntity3.class));
    }

    @Test
    public void testDeleteOneWhenShardHintManuallySet() {
        CollectionShardedReactiveMongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> ReactiveMongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(Mono.just(mockMongoDatabase));

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);

        ObjectId objectId = ObjectId.get();
        Document documentFound = new Document();
        documentFound.put("_id", objectId);
        documentFound.put(TestEntity3.Fields.indexedField, "testIndexedFieldValue");
        when(mockCollection.deleteMany(any(Document.class), any(DeleteOptions.class))).thenReturn(Flux.empty());

        TestEntity3 testEntity3 = spy(new TestEntity3());
        testEntity3.setId(ObjectId.get().toString());
        ShardingHintManager.setCollectionHint(String.valueOf(0));
        mongoTemplate.remove(testEntity3).block();

        verify(mockCollection).deleteMany(any(Document.class), any(DeleteOptions.class));
    }

    @Test(expected = UnresolvableCollectionShardException.class)
    public void testDeleteOneWhenShardHintNotSet() {
        CollectionShardedReactiveMongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        TestEntity3 testEntity3 = spy(new TestEntity3());
        testEntity3.setId(ObjectId.get().toString());
        mongoTemplate.remove(testEntity3);
    }

    @Test(expected = UnresolvableCollectionShardException.class)
    public void testDeleteOneWhenCollectionShardHintNotSet() {
        CollectionShardedReactiveMongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        TestEntity3 testEntity3 = spy(new TestEntity3());
        testEntity3.setId(ObjectId.get().toString());
        ShardingHintManager.setDatabaseHint(String.valueOf(0));
        mongoTemplate.remove(testEntity3);
    }

    @Test
    public void testGetCollection() {
        CollectionShardedReactiveMongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> ReactiveMongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(Mono.just(mockMongoDatabase));

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0")).thenReturn(mockCollection);

        ShardingHintManager.setCollectionHint(String.valueOf(0));
        Mono<MongoCollection<Document>> mongoCollection = mongoTemplate.getCollection("TEST3");
        assertEquals(mockCollection, mongoCollection.block());
    }

    @Test
    public void testGetCollectionExists() {
        CollectionShardedReactiveMongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> ReactiveMongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(Mono.just(mockMongoDatabase));

        when(mockMongoDatabase.listCollectionNames()).thenReturn(Flux.fromIterable(
                new ListCollectionsFromDatabaseIterable(Collections.singletonList("TEST3_0"))));

        ShardingHintManager.setCollectionHint(String.valueOf(0));
        assertTrue(mongoTemplate.collectionExists(TestEntity3.class).block());
    }

    @Test
    public void testGetCollectionExistsWhenPassingHint() {
        CollectionShardedReactiveMongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> ReactiveMongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(Mono.just(mockMongoDatabase));

        when(mockMongoDatabase.listCollectionNames()).thenReturn(Flux.fromIterable(
                new ListCollectionsFromDatabaseIterable(Collections.singletonList("TEST3_0"))));

        ShardingHintManager.setCollectionHint(String.valueOf(0));
        assertFalse(mongoTemplate.collectionExists("TEST3", String.valueOf(3)).block());
    }

    @Test
    public void testCreateCollection() {
        CollectionShardedReactiveMongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> ReactiveMongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(Mono.just(mockMongoDatabase));
        when(mockMongoDatabase.createCollection(eq("TEST3_0"), any())).thenReturn(Mono.empty());

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0"))
                .thenReturn(mockCollection);

        ShardingHintManager.setCollectionHint(String.valueOf(0));
        mongoTemplate.createCollection(TestEntity3.class).block();
    }

    @Test
    public void testFindById() {
        CollectionShardedReactiveMongoTemplate mongoTemplate = getFixture(FixtureConfiguration.builder().registerHintResolutionCallback(true).build());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> ReactiveMongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(Mono.just(mockMongoDatabase));

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);

        ObjectId objectId = ObjectId.get();
        Document documentFound = new Document();
        documentFound.put("_id", objectId);
        documentFound.put(TestEntity3.Fields.indexedField, "testIndexedFieldValue");

        when(mockCollection.find(any(Document.class), eq(Document.class)))
                .thenReturn(new FindFromDatabasePublisher(Collections.singletonList(documentFound)));

        Mono<TestEntity3> testEntity3Mono = mongoTemplate.findById(ObjectId.get(), TestEntity3.class);
        TestEntity3 testEntity3 = testEntity3Mono.block();
        assertNotNull(testEntity3);
        assertEquals(objectId.toString(), testEntity3.getId());

        verify((HintResolutionCallback<TestEntity3>) collectionShardingOptions.getHintResolutionCallbacks().stream().findFirst().get())
                .resolveHintForFindContext(any(Document.class), eq(TestEntity3.class));
    }

    @Test
    public void testFindByIdWhenShardingHintManuallySet() {
        CollectionShardedReactiveMongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> ReactiveMongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(Mono.just(mockMongoDatabase));

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);

        ObjectId objectId = ObjectId.get();
        Document documentFound = new Document();
        documentFound.put("_id", objectId);
        documentFound.put(TestEntity3.Fields.indexedField, "testIndexedFieldValue");

        when(mockCollection.find(any(Document.class), eq(Document.class)))
                .thenReturn(new FindFromDatabasePublisher(Collections.singletonList(documentFound)));

        ShardingHintManager.setCollectionHint(String.valueOf(0));
        Mono<TestEntity3> testEntity3Mono = mongoTemplate.findById(ObjectId.get(), TestEntity3.class);
        TestEntity3 testEntity3 = testEntity3Mono.block();
        assertNotNull(testEntity3);
        assertEquals(objectId.toString(), testEntity3.getId());
    }

    @Test
    public void testUpdate() {
        CollectionShardedReactiveMongoTemplate mongoTemplate = getFixture(FixtureConfiguration.builder().registerHintResolutionCallback(true).build());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> ReactiveMongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(Mono.just(mockMongoDatabase));

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);
        when(mockCollection.updateMany(any(Document.class), any(Document.class), any(UpdateOptions.class))).
                thenReturn(Flux.just(UpdateResult.acknowledged(1L, 1L, null)));

        Query updateQuery = new Query();
        UpdateDefinition updateDefinition = new BasicUpdate(new Document()).addToSet("indexedField", "testIndexedValue");
        mongoTemplate.updateMulti(updateQuery, updateDefinition, TestEntity3.class).block();

        verify((HintResolutionCallback<TestEntity3>) collectionShardingOptions.getHintResolutionCallbacks().stream().findFirst().get())
                .resolveHintForUpdateContext(updateQuery, updateDefinition, TestEntity3.class);
    }

    @Test
    public void testUpdateWhenShardHintManuallySet() {
        CollectionShardedReactiveMongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> ReactiveMongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(Mono.just(mockMongoDatabase));

        MongoCollection mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);
        when(mockCollection.updateMany(any(Document.class), any(Document.class), any(UpdateOptions.class))).
                thenReturn(Flux.just(UpdateResult.acknowledged(1L, 1L, null)));

        Query updateQuery = new Query();
        UpdateDefinition updateDefinition = new BasicUpdate(new Document()).addToSet("indexedField", "testIndexedValue");
        ShardingHintManager.setCollectionHint(String.valueOf(0));
        mongoTemplate.updateMulti(updateQuery, updateDefinition, TestEntity3.class).block();
    }

    @Test
    public void testFindAndRemove() {
        CollectionShardedReactiveMongoTemplate mongoTemplate = getFixture(FixtureConfiguration.builder().registerHintResolutionCallback(true).build());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> ReactiveMongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(Mono.just(mockMongoDatabase));

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);

        ObjectId objectId = ObjectId.get();
        Document documentFound = new Document();
        documentFound.put("_id", objectId);
        documentFound.put(TestEntity3.Fields.indexedField, "testIndexedFieldValue");
        when(mockCollection.findOneAndDelete(any(Document.class), any(FindOneAndDeleteOptions.class))).thenReturn(Flux.empty());

        Query deleteQuery = new Query();
        mongoTemplate.findAndRemove(deleteQuery, TestEntity3.class).block();

        verify((HintResolutionCallback<TestEntity3>) collectionShardingOptions.getHintResolutionCallbacks().stream().findFirst().get())
                .resolveHintForDeleteContext(deleteQuery.getQueryObject(), TestEntity3.class);
    }

    @Test
    public void testFindAllAndRemove() {
        CollectionShardedReactiveMongoTemplate mongoTemplate = getFixture(FixtureConfiguration.builder().registerHintResolutionCallback(true).build());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> ReactiveMongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(Mono.just(mockMongoDatabase));

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);

        ObjectId objectId = ObjectId.get();
        Document documentFound = new Document();
        documentFound.put("_id", objectId);
        documentFound.put(TestEntity3.Fields.indexedField, "testIndexedFieldValue");
        when(mockCollection.find(Document.class))
                .thenReturn(new FindFromDatabasePublisher(Collections.singletonList(documentFound)));
        when(mockCollection.deleteMany(any(Document.class), any(DeleteOptions.class))).thenReturn(Flux.empty());

        Query deleteQuery = new Query();
        mongoTemplate.findAllAndRemove(deleteQuery, TestEntity3.class).blockFirst();

        verify((HintResolutionCallback<TestEntity3>) collectionShardingOptions.getHintResolutionCallbacks().stream().findFirst().get())
                .resolveHintForDeleteContext(any(Document.class), eq(TestEntity3.class));
        verify((HintResolutionCallback<TestEntity3>) collectionShardingOptions.getHintResolutionCallbacks().stream().findFirst().get(), times(2))
                .resolveHintForFindContext(any(Document.class), eq(TestEntity3.class));
    }

    @Test
    public void testFindAllAndRemoveWhenQueryReturnsNoResults() {
        CollectionShardedReactiveMongoTemplate mongoTemplate = getFixture(FixtureConfiguration.builder().registerHintResolutionCallback(true).build());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> ReactiveMongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(Mono.just(mockMongoDatabase));

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);

        when(mockCollection.find(Document.class))
                .thenReturn(new FindFromDatabasePublisher(Collections.emptyList()));

        Query deleteQuery = new Query();
        mongoTemplate.findAllAndRemove(deleteQuery, TestEntity3.class).blockFirst();

        verify((HintResolutionCallback<TestEntity3>) collectionShardingOptions.getHintResolutionCallbacks().stream().findFirst().get())
                .resolveHintForFindContext(deleteQuery.getQueryObject(), TestEntity3.class);
    }

    @Test
    public void testFindAllAndRemoveWhenShardHintIsManuallySet() {
        CollectionShardedReactiveMongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> ReactiveMongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(Mono.just(mockMongoDatabase));

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);

        ObjectId objectId = ObjectId.get();
        Document documentFound = new Document();
        documentFound.put("_id", objectId);
        documentFound.put(TestEntity3.Fields.indexedField, "testIndexedFieldValue");
        when(mockCollection.find(Document.class))
                .thenReturn(new FindFromDatabasePublisher(Collections.singletonList(documentFound)));
        when(mockCollection.deleteMany(any(Document.class), any(DeleteOptions.class))).thenReturn(Flux.empty());

        Query deleteQuery = new Query();
        ShardingHintManager.setCollectionHint(String.valueOf(0));
        mongoTemplate.findAllAndRemove(deleteQuery, TestEntity3.class).blockFirst();
    }


    @Test
    public void testFindAndRemoveWhenShardHintManuallySet() {
        CollectionShardedReactiveMongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> ReactiveMongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(Mono.just(mockMongoDatabase));

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);
        when(mockCollection.findOneAndDelete(any(Document.class), any(FindOneAndDeleteOptions.class)))
                .thenReturn(Flux.just(new Document()));

        Query deleteQuery = new Query();
        ShardingHintManager.setCollectionHint(String.valueOf(0));
        mongoTemplate.findAndRemove(deleteQuery, TestEntity3.class).block();
    }

    @Test
    public void testFind() {
        CollectionShardedReactiveMongoTemplate mongoTemplate = getFixture(FixtureConfiguration.builder().registerHintResolutionCallback(true).build());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> ReactiveMongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(Mono.just(mockMongoDatabase));

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);

        ObjectId objectId = ObjectId.get();
        Document documentFound = new Document();
        documentFound.put("_id", objectId);
        documentFound.put(TestEntity3.Fields.indexedField, "testIndexedFieldValue");
        when(mockCollection.find(Document.class))
                .thenReturn(new FindFromDatabasePublisher(Collections.singletonList(documentFound)));

        Query findQuery = new Query();
        ShardingHintManager.setCollectionHint(String.valueOf(0));
        Flux<TestEntity3> queryResultFlux = mongoTemplate.find(findQuery, TestEntity3.class);
        TestEntity3 testEntity3 = queryResultFlux.blockFirst();

        assertEquals(objectId.toString(), testEntity3.getId());
        verify((HintResolutionCallback<TestEntity3>) collectionShardingOptions.getHintResolutionCallbacks().stream().findFirst().get())
                .resolveHintForFindContext(findQuery.getQueryObject(), TestEntity3.class);
    }

    @Test
    public void testFindWhenShardHintManuallySet() {
        CollectionShardedReactiveMongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> ReactiveMongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(Mono.just(mockMongoDatabase));

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);

        ObjectId objectId = ObjectId.get();
        Document documentFound = new Document();
        documentFound.put("_id", objectId);
        documentFound.put(TestEntity3.Fields.indexedField, "testIndexedFieldValue");
        when(mockCollection.find(Document.class))
                .thenReturn(new FindFromDatabasePublisher(Collections.singletonList(documentFound)));

        Query findQuery = new Query();
        ShardingHintManager.setCollectionHint(String.valueOf(0));
        Flux<TestEntity3> queryResult = mongoTemplate.find(findQuery, TestEntity3.class);

        TestEntity3 testEntity3 = queryResult.blockFirst();
        assertEquals(objectId.toString(), testEntity3.getId());
    }

    @Test
    public void testInsertBatch() {
        CollectionShardedReactiveMongoTemplate mongoTemplate = getFixture(FixtureConfiguration.getDefault());

        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        mongoDatabaseUtilsMockedStatic.when(() -> ReactiveMongoDatabaseUtils.getDatabase(eq(mongoTemplate.getMongoDatabaseFactory()), any()))
                .thenReturn(Mono.just(mockMongoDatabase));

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        when(mockMongoDatabase.getCollection("TEST3_0", Document.class))
                .thenReturn(mockCollection);
        when(mockCollection.insertMany(anyList()))
                .thenAnswer(invocation -> {
                    ObjectId objectId = ObjectId.get();
                    ((List<Document>) invocation.getArgument(0)).get(0).put("_id", objectId);
                    return Flux.fromIterable(Collections.singletonList(
                            InsertManyResult.acknowledged(
                                    Collections.singletonMap(0, new BsonObjectId(objectId)))));
                });

        TestEntity3 testEntity3 = spy(new TestEntity3());
        Flux<TestEntity3> persistedEntityListFlux =
                mongoTemplate.insert(Collections.singletonList(testEntity3), TestEntity3.class);

        persistedEntityListFlux.blockFirst();

        verify(testEntity3, times(1)).resolveCollectionHint();
        verify(mockCollection).insertMany(any());
    }

    @After
    public void teardown() {
        ShardingHintManager.clear();
        mongoDatabaseUtilsMockedStatic.close();
    }

    private CollectionShardedReactiveMongoTemplate getFixture(FixtureConfiguration fixtureConfiguration) {
        if (fixtureConfiguration.isRegisterHintResolutionCallback()) {
            collectionShardingOptions.setHintResolutionCallbacks(Collections.singleton(
                    spy(new TestEntity3.TestEntity3CollectionHintResolutionCallback())));
        }

        ReactiveMongoDatabaseFactory databaseFactory = mock(ReactiveMongoDatabaseFactory.class);
        databaseFactory.getMongoDatabase("TEST_DB");
        when(databaseFactory.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());

        CollectionShardedReactiveMongoTemplate collectionShardedMongoTemplate = new CollectionShardedReactiveMongoTemplate(databaseFactory, collectionShardingOptions);
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