package com.alpha.mongodb.sharding.core.template;

import com.alpha.mongodb.sharding.core.configuration.DatabaseShardingOptions;
import com.alpha.mongodb.sharding.core.exception.UnresolvableDatabaseShardException;
import com.alpha.mongodb.sharding.core.fixture.TestEntity1;
import com.alpha.mongodb.sharding.core.fixture.TestEntity3;
import com.alpha.mongodb.sharding.core.hint.ShardingHintManager;
import com.mongodb.client.MongoClient;
import lombok.Builder;
import lombok.Data;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Test;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.FindAndReplaceOptions;
import org.springframework.data.mongodb.core.MongoExceptionTranslator;
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

        MongoConverter mockMongoConverter = mock(MongoConverter.class);
        when(mockMongoConverter.getProjectionFactory()).thenReturn(mock(ProjectionFactory.class));
        when(mockMongoConverter.getMappingContext()).thenReturn(mock(MappingContext.class));
        DatabaseShardedMongoTemplate databaseShardedMongoTemplate =
                new DatabaseShardedMongoTemplate(shardedDatabaseFactoryMap, mockMongoConverter, databaseShardingOptions);
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
    }

    @Test(expected = UnresolvableDatabaseShardException.class)
    public void testFindWhenInvalidShardHint() {
        DatabaseShardedMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        Query query = new Query();
        ShardingHintManager.setDatabaseHint(String.valueOf(5));
        databaseShardedMongoTemplate.find(query, TestEntity3.class);
    }

    @Test(expected = UnresolvableDatabaseShardException.class)
    public void testFindWhenDatabaseShardHintNotSet() {
        DatabaseShardedMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        Query query = new Query();
        ShardingHintManager.setCollectionHint(String.valueOf(0));
        databaseShardedMongoTemplate.find(query, TestEntity3.class);
    }

    @Test
    public void testSave() {
        DatabaseShardedMongoTemplate databaseShardedMongoTemplate =
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
        DatabaseShardedMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        TestEntity1 testEntity1 = new TestEntity1();
        ShardingHintManager.setDatabaseHint(String.valueOf(0));
        databaseShardedMongoTemplate.save(testEntity1);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .save(testEntity1);
    }

    @Test
    public void testSaveWhenShardHintResolvedFromEntity() {
        DatabaseShardedMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        TestEntity3 testEntity3 = new TestEntity3();
        databaseShardedMongoTemplate.save(testEntity3);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .save(testEntity3);
    }

    @Test(expected = UnresolvableDatabaseShardException.class)
    public void testSaveWhenShardHintNotSet() {
        DatabaseShardedMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        TestEntity1 testEntity1 = new TestEntity1();
        databaseShardedMongoTemplate.save(testEntity1);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .save(testEntity1);
    }

    @Test(expected = UnresolvableDatabaseShardException.class)
    public void testSaveWhenCollectionShardHintSet() {
        DatabaseShardedMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        TestEntity1 testEntity1 = new TestEntity1();
        ShardingHintManager.setCollectionHint(String.valueOf(0));
        databaseShardedMongoTemplate.save(testEntity1);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .save(testEntity1);
    }

    @Test(expected = UnresolvableDatabaseShardException.class)
    public void testSaveWhenInvalidShardHintSet() {
        DatabaseShardedMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        TestEntity1 testEntity1 = new TestEntity1();
        ShardingHintManager.setCollectionHint(String.valueOf(5));
        databaseShardedMongoTemplate.save(testEntity1);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .save(testEntity1);
    }

    @Test
    public void testDelete() {
        DatabaseShardedMongoTemplate databaseShardedMongoTemplate =
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
        DatabaseShardedMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        TestEntity3 testEntity3 = new TestEntity3();
        databaseShardedMongoTemplate.remove(testEntity3);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .remove(testEntity3);
    }

    @Test
    public void testDeleteWhenShardHintManuallySet() {
        DatabaseShardedMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        TestEntity1 testEntity1 = new TestEntity1();
        ShardingHintManager.setDatabaseHint(String.valueOf(0));
        databaseShardedMongoTemplate.remove(testEntity1);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .remove(testEntity1);
    }

    @Test(expected = UnresolvableDatabaseShardException.class)
    public void testDeleteWhenShardHintNotSet() {
        DatabaseShardedMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        TestEntity1 testEntity1 = new TestEntity1();
        databaseShardedMongoTemplate.remove(testEntity1);
    }

    @Test(expected = UnresolvableDatabaseShardException.class)
    public void testDeleteWhenCollectionShardHintSet() {
        DatabaseShardedMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        TestEntity1 testEntity1 = new TestEntity1();
        ShardingHintManager.setCollectionHint(String.valueOf(0));
        databaseShardedMongoTemplate.remove(testEntity1);
    }

    @Test(expected = UnresolvableDatabaseShardException.class)
    public void testDeleteWhenInvalidShardHintSet() {
        DatabaseShardedMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        TestEntity1 testEntity1 = new TestEntity1();
        ShardingHintManager.setDatabaseHint(String.valueOf(5));
        databaseShardedMongoTemplate.remove(testEntity1);
    }

    @Test
    public void testUpdate() {
        DatabaseShardedMongoTemplate databaseShardedMongoTemplate =
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
        DatabaseShardedMongoTemplate databaseShardedMongoTemplate =
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
        DatabaseShardedMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        Query query = new Query();
        UpdateDefinition basicUpdate = new BasicUpdate(new Document());
        databaseShardedMongoTemplate.updateFirst(query, basicUpdate, TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .updateFirst(query, basicUpdate, TestEntity3.class);
    }

    @Test(expected = UnresolvableDatabaseShardException.class)
    public void testUpdateWhenCollectionShardHintSet() {
        DatabaseShardedMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        Query query = new Query();
        UpdateDefinition basicUpdate = new BasicUpdate(new Document());
        ShardingHintManager.setCollectionHint(String.valueOf(0));
        databaseShardedMongoTemplate.updateFirst(query, basicUpdate, TestEntity3.class);
    }

    @Test(expected = UnresolvableDatabaseShardException.class)
    public void testUpdateWhenInvalidShardHintSet() {
        DatabaseShardedMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.getDefault());

        Query query = new Query();
        UpdateDefinition basicUpdate = new BasicUpdate(new Document());
        ShardingHintManager.setDatabaseHint(String.valueOf(5));
        databaseShardedMongoTemplate.updateFirst(query, basicUpdate, TestEntity3.class);
    }

    @Test
    public void testStream() {
        DatabaseShardedMongoTemplate databaseShardedMongoTemplate =
                getFixture(FixtureConfiguration.builder().registerHintResolutionCallback(true).build());

        Query query = new Query();
        databaseShardedMongoTemplate.stream(query, TestEntity3.class);
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .stream(query, TestEntity3.class);

        databaseShardedMongoTemplate.stream(query, TestEntity3.class, "TEST3");
        verify(databaseShardedMongoTemplate.getDelegatedShardedMongoTemplateMap().get(String.valueOf(0)))
                .stream(query, TestEntity3.class, "TEST3");
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

        ExtendedMongoTemplate mongoTemplate0 = mock(ExtendedMongoTemplate.class);
        ExtendedMongoTemplate mongoTemplate1 = mock(ExtendedMongoTemplate.class);
        ExtendedMongoTemplate mongoTemplate2 = mock(ExtendedMongoTemplate.class);

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