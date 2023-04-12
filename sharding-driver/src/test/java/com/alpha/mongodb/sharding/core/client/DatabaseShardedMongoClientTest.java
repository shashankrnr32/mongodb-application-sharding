package com.alpha.mongodb.sharding.core.client;

import com.alpha.mongodb.sharding.core.configuration.DatabaseShardingOptions;
import com.alpha.mongodb.sharding.core.exception.UnresolvableDatabaseShardException;
import com.alpha.mongodb.sharding.core.fixture.TestEntity1;
import com.alpha.mongodb.sharding.core.hint.ShardingHintManager;
import com.mongodb.ClientSessionOptions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DatabaseShardedMongoClientTest {

    DatabaseShardingOptions databaseShardingOptions =
            DatabaseShardingOptions.withIntegerStreamHints(IntStream.range(0, 3));

    MongoClient mockMongoClient = mock(MongoClient.class);

    ClientSessionOptions mockClientSessionOptions = mock(ClientSessionOptions.class);
    ClientSession mockClientSession = mock(ClientSession.class);

    @Before
    public void setup() {
        ShardingHintManager.setDatabaseHint(String.valueOf(0));
    }

    @Test
    public void testGetDatabase() {
        MongoDatabase mockMongoDatabase = mock(MongoDatabase.class);
        when(mockMongoClient.getDatabase("TEST_0")).thenReturn(mockMongoDatabase);
        MongoDatabase database = getFixture().getDatabase("TEST");
        verify(mockMongoClient).getDatabase("TEST_0");
    }

    @Test(expected = UnresolvableDatabaseShardException.class)
    public void testGetDatabaseWhenShardHintNotSet() {
        ShardingHintManager.clear();
        MongoDatabase database = getFixture().getDatabase("TEST");
        verify(mockMongoClient).getDatabase("TEST_0");
    }

    @Test
    public void testStartSession() {
        getFixture().startSession();
        verify(mockMongoClient).startSession();
    }

    @Test
    public void testStartSessionWithClientSessionOptions() {
        getFixture().startSession(mockClientSessionOptions);
        verify(mockMongoClient).startSession(mockClientSessionOptions);
    }

    @Test
    public void testClose() {
        getFixture().close();
        verify(mockMongoClient).close();
    }

    @Test
    public void testListDatabaseNames() {
        getFixture().listDatabaseNames();
        verify(mockMongoClient).listDatabaseNames();
    }

    @Test
    public void testListDatabaseNamesWithClientSession() {
        getFixture().listDatabaseNames(mockClientSession);
        verify(mockMongoClient).listDatabaseNames(mockClientSession);
    }

    @Test
    public void testListDatabases() {
        getFixture().listDatabases();
        verify(mockMongoClient).listDatabases();
    }

    @Test
    public void testListDatabasesWithEntity() {
        getFixture().listDatabases(TestEntity1.class);
        verify(mockMongoClient).listDatabases(TestEntity1.class);
    }

    @Test
    public void testListDatabasesWithClientSession() {
        getFixture().listDatabases(mockClientSession);
        verify(mockMongoClient).listDatabases(mockClientSession);

    }

    @Test
    public void testListDatabasesWithClientSessionAndEntityClass() {
        getFixture().listDatabases(mockClientSession, TestEntity1.class);
        verify(mockMongoClient).listDatabases(mockClientSession, TestEntity1.class);
    }

    @Test
    public void testWatch() {
        getFixture().watch();
        verify(mockMongoClient).watch();
    }

    @Test
    public void testWatchWithEntityClass() {
        getFixture().watch(TestEntity1.class);
        verify(mockMongoClient).watch(TestEntity1.class);
    }

    @Test
    public void testWatchWithClientSession() {
        getFixture().watch(mockClientSession);
        verify(mockMongoClient).watch(mockClientSession);
    }

    @Test
    public void testWatchWithList() {
        getFixture().watch(new ArrayList<>());
        verify(mockMongoClient).watch(anyList());
    }

    @Test
    public void testWatchWithListAndEntityCLass() {
        getFixture().watch(new ArrayList<>(), TestEntity1.class);
        verify(mockMongoClient).watch(anyList(), eq(TestEntity1.class));
    }

    @Test
    public void testWatchWithClientSessionAndEntityClass() {
        getFixture().watch(mockClientSession, TestEntity1.class);
        verify(mockMongoClient).watch(mockClientSession, TestEntity1.class);
    }

    @Test
    public void testWatchWithClientSessionAndList() {
        getFixture().watch(mockClientSession, new ArrayList<>());
        verify(mockMongoClient).watch(eq(mockClientSession), anyList());
    }

    @Test
    public void testWatchWithCLientSessionAndListAndEntityClass() {
        getFixture().watch(mockClientSession, new ArrayList<>(), TestEntity1.class);
        verify(mockMongoClient).watch(eq(mockClientSession), anyList(), eq(TestEntity1.class));
    }

    @Test
    public void getClusterDescription() {
        getFixture().getClusterDescription();
        verify(mockMongoClient).getClusterDescription();
    }

    @After
    public void teardown() {
        ShardingHintManager.clear();
    }

    public MongoClient getFixture() {
        return new DatabaseShardedMongoClient(mockMongoClient, databaseShardingOptions);
    }
}