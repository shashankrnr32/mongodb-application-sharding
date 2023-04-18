package com.alpha.mongodb.sharding.core.template;

import com.alpha.mongodb.sharding.core.configuration.DatabaseShardingOptions;
import com.alpha.mongodb.sharding.core.exception.UnresolvableDatabaseShardException;
import com.alpha.mongodb.sharding.core.fixture.TestEntity1;
import com.alpha.mongodb.sharding.core.fixture.TestEntity3;
import com.alpha.mongodb.sharding.core.hint.ShardingHintManager;
import com.mongodb.reactivestreams.client.MongoClient;
import lombok.Builder;
import lombok.Data;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Test;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.FindAndReplaceOptions;
import org.springframework.data.mongodb.core.MongoExceptionTranslator;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.query.BasicUpdate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.projection.ProjectionFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DatabaseShardedReactiveMongoTemplateTest {

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

        DatabaseShardedReactiveMongoTemplate databaseShardedMongoTemplate =
                new DatabaseShardedReactiveMongoTemplate(mongoClientMap, "TEST_DB", databaseShardingOptions);
        assertNotNull(databaseShardedMongoTemplate);
        assertEquals(3, databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().size());
    }

    @Test
    public void testConstructorsWithMongoClient() {
        MongoClient mongoClient = mock(MongoClient.class);

        DatabaseShardedReactiveMongoTemplate databaseShardedMongoTemplate =
                new DatabaseShardedReactiveMongoTemplate(mongoClient, "TEST_DB", databaseShardingOptions);
        assertNotNull(databaseShardedMongoTemplate);
        assertEquals(3, databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().size());
    }

    @Test
    public void testConstructorWithMongoDatabaseFactoryMapAndConverter() {
        ReactiveMongoDatabaseFactory databaseFactory0 = mock(ReactiveMongoDatabaseFactory.class);
        databaseFactory0.getMongoDatabase("TEST_DB_0");
        when(databaseFactory0.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());

        ReactiveMongoDatabaseFactory databaseFactory1 = mock(ReactiveMongoDatabaseFactory.class);
        databaseFactory0.getMongoDatabase("TEST_DB_1");
        when(databaseFactory0.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());

        ReactiveMongoDatabaseFactory databaseFactory2 = mock(ReactiveMongoDatabaseFactory.class);
        databaseFactory0.getMongoDatabase("TEST_DB_2");
        when(databaseFactory0.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());

        Map<String, ReactiveMongoDatabaseFactory> shardedDatabaseFactoryMap = new HashMap<>();
        shardedDatabaseFactoryMap.put(String.valueOf(0), databaseFactory0);
        shardedDatabaseFactoryMap.put(String.valueOf(1), databaseFactory1);
        shardedDatabaseFactoryMap.put(String.valueOf(2), databaseFactory2);

        MongoConverter mockMongoConverter = mock(MongoConverter.class);
        when(mockMongoConverter.getProjectionFactory()).thenReturn(mock(ProjectionFactory.class));
        when(mockMongoConverter.getMappingContext()).thenReturn(mock(MappingContext.class));
        DatabaseShardedReactiveMongoTemplate databaseShardedMongoTemplate =
                new DatabaseShardedReactiveMongoTemplate(shardedDatabaseFactoryMap, mockMongoConverter, databaseShardingOptions);
        assertEquals(databaseShardingOptions, databaseShardedMongoTemplate.getShardingOptions());
        assertNotNull(databaseShardedMongoTemplate);
    }


    @Test
    public void testFind() {
        DatabaseShardedReactiveMongoTemplate databaseShardedMongoTemplate =
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

        ObjectId objectId = ObjectId.get();
        databaseShardedMongoTemplate.findById(objectId, TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .findById(objectId, TestEntity3.class);

        databaseShardedMongoTemplate.findById(objectId, TestEntity3.class, "TEST3");
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .findById(objectId, TestEntity3.class, "TEST3");

        databaseShardedMongoTemplate.findOne(query, TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .findOne(query, TestEntity3.class);

        databaseShardedMongoTemplate.findOne(query, TestEntity3.class, "TEST3");
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .findOne(query, TestEntity3.class, "TEST3");
    }

    @Test
    public void testFindWhenShardHintManuallySet() {
        DatabaseShardedReactiveMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        Query query = new Query();
        ShardingHintManager.setDatabaseHint(String.valueOf(0));
        databaseShardedMongoTemplate.find(query, TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .find(query, TestEntity3.class);
    }

    @Test(expected = UnresolvableDatabaseShardException.class)
    public void testFindWhenShardHintNotSet() {
        DatabaseShardedReactiveMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        Query query = new Query();
        databaseShardedMongoTemplate.find(query, TestEntity3.class);
    }

    @Test(expected = UnresolvableDatabaseShardException.class)
    public void testFindWhenInvalidShardHint() {
        DatabaseShardedReactiveMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        Query query = new Query();
        ShardingHintManager.setDatabaseHint(String.valueOf(5));
        databaseShardedMongoTemplate.find(query, TestEntity3.class);
    }

    @Test(expected = UnresolvableDatabaseShardException.class)
    public void testFindWhenDatabaseShardHintNotSet() {
        DatabaseShardedReactiveMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        Query query = new Query();
        ShardingHintManager.setCollectionHint(String.valueOf(0));
        databaseShardedMongoTemplate.find(query, TestEntity3.class);
    }

    @Test
    public void testSave() {
        DatabaseShardedReactiveMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.builder().registerHintResolutionCallback(true).build());

        TestEntity3 testEntity3 = new TestEntity3();
        databaseShardedMongoTemplate.save(testEntity3);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .save(testEntity3);

        databaseShardedMongoTemplate.save(testEntity3, "TEST3");
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .save(testEntity3, "TEST3");

        databaseShardedMongoTemplate.insert(testEntity3);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .insert(testEntity3);

        databaseShardedMongoTemplate.insert(testEntity3, "TEST3");
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .insert(testEntity3, "TEST3");

        databaseShardedMongoTemplate.insert(Collections.singletonList(testEntity3), TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .insert(anyList(), eq(TestEntity3.class));

        databaseShardedMongoTemplate.insert(Collections.singletonList(testEntity3), "TEST3");
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .insert(anyList(), eq("TEST3"));

        databaseShardedMongoTemplate.insertAll(Collections.singletonList(testEntity3));
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .insertAll(anyList());
    }

    @Test
    public void testSaveWhenShardHintManuallySet() {
        DatabaseShardedReactiveMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        TestEntity1 testEntity1 = new TestEntity1();
        ShardingHintManager.setDatabaseHint(String.valueOf(0));
        databaseShardedMongoTemplate.save(testEntity1);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .save(testEntity1);
    }

    @Test
    public void testSaveWhenShardHintResolvedFromEntity() {
        DatabaseShardedReactiveMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        TestEntity3 testEntity3 = new TestEntity3();
        databaseShardedMongoTemplate.save(testEntity3);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .save(testEntity3);
    }

    @Test(expected = UnresolvableDatabaseShardException.class)
    public void testSaveWhenShardHintNotSet() {
        DatabaseShardedReactiveMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        TestEntity1 testEntity1 = new TestEntity1();
        databaseShardedMongoTemplate.save(testEntity1);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .save(testEntity1);
    }

    @Test(expected = UnresolvableDatabaseShardException.class)
    public void testSaveWhenCollectionShardHintSet() {
        DatabaseShardedReactiveMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        TestEntity1 testEntity1 = new TestEntity1();
        ShardingHintManager.setCollectionHint(String.valueOf(0));
        databaseShardedMongoTemplate.save(testEntity1);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .save(testEntity1);
    }

    @Test(expected = UnresolvableDatabaseShardException.class)
    public void testSaveWhenInvalidShardHintSet() {
        DatabaseShardedReactiveMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        TestEntity1 testEntity1 = new TestEntity1();
        ShardingHintManager.setCollectionHint(String.valueOf(5));
        databaseShardedMongoTemplate.save(testEntity1);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .save(testEntity1);
    }

    @Test
    public void testDelete() {
        DatabaseShardedReactiveMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.builder().registerHintResolutionCallback(true).build());

        TestEntity3 testEntity3 = new TestEntity3();
        databaseShardedMongoTemplate.remove(testEntity3);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .remove(testEntity3);

        Query query = new Query();
        databaseShardedMongoTemplate.remove(query, TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .remove(query, TestEntity3.class);

        ShardingHintManager.setDatabaseHint(String.valueOf(0));
        databaseShardedMongoTemplate.remove(query, "TEST3");
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .remove(query, "TEST3");

        databaseShardedMongoTemplate.remove(query, TestEntity3.class, "TEST3");
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .remove(query, TestEntity3.class, "TEST3");

        databaseShardedMongoTemplate.remove(testEntity3, "TEST3");
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .remove(testEntity3, "TEST3");

        databaseShardedMongoTemplate.findAllAndRemove(query, TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .findAllAndRemove(query, TestEntity3.class);

        databaseShardedMongoTemplate.findAllAndRemove(query, "TEST3");
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .findAllAndRemove(query, "TEST3");

        databaseShardedMongoTemplate.findAllAndRemove(query, TestEntity3.class, "TEST3");
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .findAllAndRemove(query, TestEntity3.class, "TEST3");

        databaseShardedMongoTemplate.findAndRemove(query, TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .findAndRemove(query, TestEntity3.class);

        databaseShardedMongoTemplate.findAndRemove(query, TestEntity3.class, "TEST3");
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .findAndRemove(query, TestEntity3.class, "TEST3");
    }

    @Test
    public void testDeleteWhenShardHintResolvedFromEntity() {
        DatabaseShardedReactiveMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        TestEntity3 testEntity3 = new TestEntity3();
        databaseShardedMongoTemplate.remove(testEntity3);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .remove(testEntity3);
    }

    @Test
    public void testDeleteWhenShardHintManuallySet() {
        DatabaseShardedReactiveMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        TestEntity1 testEntity1 = new TestEntity1();
        ShardingHintManager.setDatabaseHint(String.valueOf(0));
        databaseShardedMongoTemplate.remove(testEntity1);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .remove(testEntity1);
    }

    @Test(expected = UnresolvableDatabaseShardException.class)
    public void testDeleteWhenShardHintNotSet() {
        DatabaseShardedReactiveMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        TestEntity1 testEntity1 = new TestEntity1();
        databaseShardedMongoTemplate.remove(testEntity1);
    }

    @Test(expected = UnresolvableDatabaseShardException.class)
    public void testDeleteWhenCollectionShardHintSet() {
        DatabaseShardedReactiveMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        TestEntity1 testEntity1 = new TestEntity1();
        ShardingHintManager.setCollectionHint(String.valueOf(0));
        databaseShardedMongoTemplate.remove(testEntity1);
    }

    @Test(expected = UnresolvableDatabaseShardException.class)
    public void testDeleteWhenInvalidShardHintSet() {
        DatabaseShardedReactiveMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        TestEntity1 testEntity1 = new TestEntity1();
        ShardingHintManager.setDatabaseHint(String.valueOf(5));
        databaseShardedMongoTemplate.remove(testEntity1);
    }

    @Test
    public void testUpdate() {
        DatabaseShardedReactiveMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.builder().registerHintResolutionCallback(true).build());
        ShardingHintManager.setDatabaseHint(String.valueOf(0));

        Query query = new Query();
        UpdateDefinition basicUpdate = new BasicUpdate(new Document());
        databaseShardedMongoTemplate.updateFirst(query, basicUpdate, TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .updateFirst(query, basicUpdate, TestEntity3.class);

        databaseShardedMongoTemplate.updateFirst(query, basicUpdate, "TEST3");
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .updateFirst(query, basicUpdate, "TEST3");

        databaseShardedMongoTemplate.updateFirst(query, basicUpdate, TestEntity3.class, "TEST3");
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .updateFirst(query, basicUpdate, TestEntity3.class, "TEST3");

        databaseShardedMongoTemplate.upsert(query, basicUpdate, TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .upsert(query, basicUpdate, TestEntity3.class);

        databaseShardedMongoTemplate.upsert(query, basicUpdate, "TEST3");
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .upsert(query, basicUpdate, "TEST3");

        databaseShardedMongoTemplate.upsert(query, basicUpdate, TestEntity3.class, "TEST3");
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .upsert(query, basicUpdate, TestEntity3.class, "TEST3");

        databaseShardedMongoTemplate.updateMulti(query, basicUpdate, TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .updateMulti(query, basicUpdate, TestEntity3.class);

        databaseShardedMongoTemplate.updateMulti(query, basicUpdate, "TEST3");
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .updateMulti(query, basicUpdate, "TEST3");

        databaseShardedMongoTemplate.updateMulti(query, basicUpdate, TestEntity3.class, "TEST3");
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .updateMulti(query, basicUpdate, TestEntity3.class, "TEST3");

        databaseShardedMongoTemplate.findAndModify(query, basicUpdate, TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .findAndModify(query, basicUpdate, TestEntity3.class);

        databaseShardedMongoTemplate.findAndModify(query, basicUpdate, TestEntity3.class, "TEST3");
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .findAndModify(query, basicUpdate, TestEntity3.class, "TEST3");

        FindAndModifyOptions findAndModifyOptions = new FindAndModifyOptions();
        databaseShardedMongoTemplate.findAndModify(query, basicUpdate, findAndModifyOptions, TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .findAndModify(query, basicUpdate, findAndModifyOptions, TestEntity3.class);

        databaseShardedMongoTemplate.findAndModify(query, basicUpdate, findAndModifyOptions, TestEntity3.class, "TEST3");
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .findAndModify(query, basicUpdate, findAndModifyOptions, TestEntity3.class, "TEST3");

        TestEntity3 testEntity3 = new TestEntity3();
        databaseShardedMongoTemplate.findAndReplace(query, testEntity3);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .findAndReplace(query, testEntity3);

        databaseShardedMongoTemplate.findAndReplace(query, testEntity3, "TEST3");
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .findAndReplace(query, testEntity3, "TEST3");

        FindAndReplaceOptions findAndReplaceOptions = new FindAndReplaceOptions();
        databaseShardedMongoTemplate.findAndReplace(query, testEntity3, findAndReplaceOptions);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .findAndReplace(query, testEntity3, findAndReplaceOptions);

        databaseShardedMongoTemplate.findAndReplace(query, testEntity3, findAndReplaceOptions, "TEST3");
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .findAndReplace(query, testEntity3, findAndReplaceOptions, "TEST3");

        databaseShardedMongoTemplate.findAndReplace(query, testEntity3, findAndReplaceOptions, TestEntity3.class, TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .findAndReplace(query, testEntity3, findAndReplaceOptions, TestEntity3.class, TestEntity3.class);

        databaseShardedMongoTemplate.findAndReplace(query, testEntity3, findAndReplaceOptions, TestEntity3.class, "TEST3");
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .findAndReplace(query, testEntity3, findAndReplaceOptions, TestEntity3.class, "TEST3");

        databaseShardedMongoTemplate.findAndReplace(query, testEntity3, findAndReplaceOptions, TestEntity3.class, "TEST3", TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .findAndReplace(query, testEntity3, findAndReplaceOptions, TestEntity3.class, "TEST3", TestEntity3.class);

    }

    @Test
    public void testUpdateWhenShardHintManuallySet() {
        DatabaseShardedReactiveMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        Query query = new Query();
        UpdateDefinition basicUpdate = new BasicUpdate(new Document());
        ShardingHintManager.setDatabaseHint(String.valueOf(0));
        databaseShardedMongoTemplate.updateFirst(query, basicUpdate, TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .updateFirst(query, basicUpdate, TestEntity3.class);
    }

    @Test(expected = UnresolvableDatabaseShardException.class)
    public void testUpdateWhenShardHintNotSet() {
        DatabaseShardedReactiveMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        Query query = new Query();
        UpdateDefinition basicUpdate = new BasicUpdate(new Document());
        databaseShardedMongoTemplate.updateFirst(query, basicUpdate, TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .updateFirst(query, basicUpdate, TestEntity3.class);
    }

    @Test(expected = UnresolvableDatabaseShardException.class)
    public void testUpdateWhenCollectionShardHintSet() {
        DatabaseShardedReactiveMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        Query query = new Query();
        UpdateDefinition basicUpdate = new BasicUpdate(new Document());
        ShardingHintManager.setCollectionHint(String.valueOf(0));
        databaseShardedMongoTemplate.updateFirst(query, basicUpdate, TestEntity3.class);
    }

    @Test(expected = UnresolvableDatabaseShardException.class)
    public void testUpdateWhenInvalidShardHintSet() {
        DatabaseShardedReactiveMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        Query query = new Query();
        UpdateDefinition basicUpdate = new BasicUpdate(new Document());
        ShardingHintManager.setDatabaseHint(String.valueOf(5));
        databaseShardedMongoTemplate.updateFirst(query, basicUpdate, TestEntity3.class);
    }

    @Test
    public void testExecutables() {
        DatabaseShardedReactiveMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.builder().registerHintResolutionCallback(true).build());

        ShardingHintManager.setDatabaseHint(String.valueOf(0));

        databaseShardedMongoTemplate.remove(TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .remove(TestEntity3.class);

        databaseShardedMongoTemplate.insert(TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .insert(TestEntity3.class);

        databaseShardedMongoTemplate.update(TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .update(TestEntity3.class);

        databaseShardedMongoTemplate.findAll(TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .findAll(TestEntity3.class);

        databaseShardedMongoTemplate.query(TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .query(TestEntity3.class);
    }


    @After
    public void teardown() {
        ShardingHintManager.clear();
    }

    private DatabaseShardedReactiveMongoTemplate getFixture(FixtureConfiguration fixtureConfiguration) {
        if (fixtureConfiguration.isRegisterHintResolutionCallback()) {
            databaseShardingOptions.setHintResolutionCallbacks(Collections.singleton(
                    spy(new TestEntity3.TestEntity3DatabaseHintResolutionCallback())));
        }

        ReactiveMongoDatabaseFactory databaseFactory0 = mock(ReactiveMongoDatabaseFactory.class);
        databaseFactory0.getMongoDatabase("TEST_DB_0");
        when(databaseFactory0.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());

        ReactiveMongoDatabaseFactory databaseFactory1 = mock(ReactiveMongoDatabaseFactory.class);
        databaseFactory0.getMongoDatabase("TEST_DB_1");
        when(databaseFactory0.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());

        ReactiveMongoDatabaseFactory databaseFactory2 = mock(ReactiveMongoDatabaseFactory.class);
        databaseFactory0.getMongoDatabase("TEST_DB_2");
        when(databaseFactory0.getExceptionTranslator()).thenReturn(new MongoExceptionTranslator());

        Map<String, ReactiveMongoDatabaseFactory> shardedDatabaseFactoryMap = new HashMap<>();
        shardedDatabaseFactoryMap.put(String.valueOf(0), databaseFactory0);
        shardedDatabaseFactoryMap.put(String.valueOf(1), databaseFactory1);
        shardedDatabaseFactoryMap.put(String.valueOf(2), databaseFactory2);

        DatabaseShardedReactiveMongoTemplate databaseShardedMongoTemplate =
                spy(new DatabaseShardedReactiveMongoTemplate(shardedDatabaseFactoryMap, databaseShardingOptions));
        assertEquals(databaseShardingOptions, databaseShardedMongoTemplate.getShardingOptions());
        assertNotNull(databaseShardedMongoTemplate);

        ReactiveMongoTemplate reactiveMongoTemplate0 = mock(ReactiveMongoTemplate.class);
        ReactiveMongoTemplate reactiveMongoTemplate1 = mock(ReactiveMongoTemplate.class);
        ReactiveMongoTemplate reactiveMongoTemplate2 = mock(ReactiveMongoTemplate.class);

        databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().put(String.valueOf(0), reactiveMongoTemplate0);
        databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().put(String.valueOf(1), reactiveMongoTemplate1);
        databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().put(String.valueOf(2), reactiveMongoTemplate2);

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