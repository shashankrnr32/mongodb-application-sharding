package com.alpha.mongodb.sharding.core.database;

import com.alpha.mongodb.sharding.core.configuration.CollectionShardingOptions;
import com.alpha.mongodb.sharding.core.fixture.TestEntity1;
import com.alpha.mongodb.sharding.core.hint.ShardingHintManager;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateViewOptions;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.stream.IntStream;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class CollectionShardedMongoDatabaseTest {

    CollectionShardingOptions collectionShardingOptions =
            CollectionShardingOptions.withIntegerStreamHints(IntStream.range(0, 3));

    MongoDatabase delegatedMongoDatabase = mock(MongoDatabase.class);

    ClientSession clientSession = mock(ClientSession.class);
    ReadPreference readPreference = mock(ReadPreference.class);
    CreateViewOptions createViewOptions = mock(CreateViewOptions.class);
    CreateCollectionOptions createCollectionOptions = mock(CreateCollectionOptions.class);

    @Before
    public void setup() {
        ShardingHintManager.setCollectionHint(String.valueOf(0));
    }

    @Test
    public void getCollection() {
        getFixture().getCollection("TEST1");
        verify(delegatedMongoDatabase).getCollection("TEST1_0");

        getFixture().getCollection("TEST1", TestEntity1.class);
        verify(delegatedMongoDatabase).getCollection("TEST1_0", TestEntity1.class);
    }

    @Test
    public void createCollection() {
        getFixture().createCollection("TEST1");
        verify(delegatedMongoDatabase).createCollection("TEST1_0");

        getFixture().createCollection("TEST1", createCollectionOptions);
        verify(delegatedMongoDatabase).createCollection("TEST1_0", createCollectionOptions);

        getFixture().createCollection(clientSession, "TEST1");
        verify(delegatedMongoDatabase).createCollection(clientSession, "TEST1_0");

        getFixture().createCollection(clientSession, "TEST1", createCollectionOptions);
        verify(delegatedMongoDatabase).createCollection(clientSession, "TEST1_0", createCollectionOptions);
    }

    @Test
    public void testNestedGetters() {
        assertTrue(getFixture().withCodecRegistry(mock(CodecRegistry.class))
                instanceof CollectionShardedMongoDatabase);

        assertTrue(getFixture().withReadConcern(mock(ReadConcern.class))
                instanceof CollectionShardedMongoDatabase);

        assertTrue(getFixture().withWriteConcern(mock(WriteConcern.class))
                instanceof CollectionShardedMongoDatabase);

        assertTrue(getFixture().withReadPreference(mock(ReadPreference.class))
                instanceof CollectionShardedMongoDatabase);
    }

    @Test
    public void testDelegatedMethods() {
        CollectionShardedMongoDatabase fixture = getFixture();

        Document document = new Document();

        fixture.runCommand(clientSession, document);
        verify(delegatedMongoDatabase).runCommand(clientSession, document);

        fixture.runCommand(clientSession, document, TestEntity1.class);
        verify(delegatedMongoDatabase).runCommand(clientSession, document, TestEntity1.class);

        fixture.runCommand(clientSession, document, readPreference);
        verify(delegatedMongoDatabase).runCommand(clientSession, document, readPreference);

        fixture.runCommand(clientSession, document, readPreference, TestEntity1.class);
        verify(delegatedMongoDatabase).runCommand(clientSession, document, readPreference, TestEntity1.class);

        fixture.runCommand(document);
        verify(delegatedMongoDatabase).runCommand(document);

        fixture.runCommand(document, readPreference);
        verify(delegatedMongoDatabase).runCommand(document, readPreference);

        fixture.runCommand(document, TestEntity1.class);
        verify(delegatedMongoDatabase).runCommand(document, TestEntity1.class);

        fixture.runCommand(document, readPreference, TestEntity1.class);
        verify(delegatedMongoDatabase).runCommand(document, readPreference, TestEntity1.class);

        fixture.watch();
        verify(delegatedMongoDatabase).watch();

        fixture.watch(TestEntity1.class);
        verify(delegatedMongoDatabase).watch(TestEntity1.class);

        fixture.watch(Collections.singletonList(document));
        verify(delegatedMongoDatabase).watch(anyList());

        fixture.watch(Collections.singletonList(document), TestEntity1.class);
        verify(delegatedMongoDatabase).watch(anyList(), eq(TestEntity1.class));

        fixture.watch(clientSession);
        verify(delegatedMongoDatabase).watch(clientSession);

        fixture.watch(clientSession, TestEntity1.class);
        verify(delegatedMongoDatabase).watch(clientSession, TestEntity1.class);

        fixture.watch(clientSession, Collections.singletonList(document));
        verify(delegatedMongoDatabase).watch(eq(clientSession), anyList());

        fixture.watch(clientSession, Collections.singletonList(document), TestEntity1.class);
        verify(delegatedMongoDatabase).watch(eq(clientSession), anyList(), eq(TestEntity1.class));

        fixture.aggregate(Collections.singletonList(document));
        verify(delegatedMongoDatabase).aggregate(anyList());

        fixture.aggregate(Collections.singletonList(document), TestEntity1.class);
        verify(delegatedMongoDatabase).aggregate(anyList(), eq(TestEntity1.class));

        fixture.aggregate(clientSession, Collections.singletonList(document));
        verify(delegatedMongoDatabase).aggregate(eq(clientSession), anyList());

        fixture.aggregate(clientSession, Collections.singletonList(document), TestEntity1.class);
        verify(delegatedMongoDatabase).aggregate(eq(clientSession), anyList(), eq(TestEntity1.class));

        fixture.drop();
        verify(delegatedMongoDatabase).drop();

        fixture.drop(clientSession);
        verify(delegatedMongoDatabase).drop(clientSession);

        fixture.createView("testViewName", "testViewOn", Collections.singletonList(document));
        verify(delegatedMongoDatabase).createView(eq("testViewName"), eq("testViewOn"), anyList());

        fixture.createView("testViewName", "testViewOn", Collections.singletonList(document), createViewOptions);
        verify(delegatedMongoDatabase).createView(eq("testViewName"), eq("testViewOn"), anyList(), eq(createViewOptions));

        fixture.createView(clientSession, "testViewName", "testViewOn", Collections.singletonList(document));
        verify(delegatedMongoDatabase).createView(eq(clientSession), eq("testViewName"), eq("testViewOn"), anyList());

        fixture.createView(clientSession, "testViewName", "testViewOn", Collections.singletonList(document), createViewOptions);
        verify(delegatedMongoDatabase).createView(eq(clientSession), eq("testViewName"), eq("testViewOn"), anyList(), eq(createViewOptions));

        fixture.listCollections(TestEntity1.class);
        verify(delegatedMongoDatabase).listCollections(TestEntity1.class);

        fixture.listCollections(clientSession);
        verify(delegatedMongoDatabase).listCollections(clientSession);

        fixture.listCollections();
        verify(delegatedMongoDatabase).listCollections();

        fixture.listCollections(clientSession, TestEntity1.class);
        verify(delegatedMongoDatabase).listCollections(clientSession, TestEntity1.class);

        fixture.listCollectionNames();
        verify(delegatedMongoDatabase).listCollectionNames();

        fixture.listCollectionNames(clientSession);
        verify(delegatedMongoDatabase).listCollectionNames(clientSession);

        fixture.getName();
        verify(delegatedMongoDatabase).getName();

        fixture.getCodecRegistry();
        verify(delegatedMongoDatabase).getCodecRegistry();

        fixture.getReadConcern();
        verify(delegatedMongoDatabase).getReadConcern();

        fixture.getWriteConcern();
        verify(delegatedMongoDatabase).getWriteConcern();

        fixture.getReadPreference();
        verify(delegatedMongoDatabase).getReadPreference();
    }

    public CollectionShardedMongoDatabase getFixture() {
        return new CollectionShardedMongoDatabase(delegatedMongoDatabase, collectionShardingOptions);
    }

    @After
    public void teardown() {
        ShardingHintManager.clear();
    }

}