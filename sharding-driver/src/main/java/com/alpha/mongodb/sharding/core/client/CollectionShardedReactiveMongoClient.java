package com.alpha.mongodb.sharding.core.client;

import com.alpha.mongodb.sharding.core.configuration.CollectionShardingOptions;
import com.mongodb.ClientSessionOptions;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.ListDatabasesPublisher;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoDatabase;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import java.util.List;

public class CollectionShardedReactiveMongoClient implements ShardedReactiveMongoClient {
    private final CollectionShardingOptions shardingOptions;

    private final MongoClient delegatedMongoClient;

    public CollectionShardedReactiveMongoClient(MongoClient mongoClient,
                                                CollectionShardingOptions shardingOptions) {
        super();
        this.shardingOptions = shardingOptions;
        this.delegatedMongoClient = mongoClient;
    }

    @Override
    public MongoDatabase getDatabase(String s) {
        return delegatedMongoClient.getDatabase(s);
    }

    @Override
    public void close() {
        delegatedMongoClient.close();
    }

    @Override
    public Publisher<String> listDatabaseNames() {
        return delegatedMongoClient.listDatabaseNames();
    }

    @Override
    public Publisher<String> listDatabaseNames(ClientSession clientSession) {
        return delegatedMongoClient.listDatabaseNames(clientSession);
    }

    @Override
    public ListDatabasesPublisher<Document> listDatabases() {
        return delegatedMongoClient.listDatabases();
    }

    @Override
    public <T> ListDatabasesPublisher<T> listDatabases(Class<T> aClass) {
        return delegatedMongoClient.listDatabases(aClass);
    }

    @Override
    public ListDatabasesPublisher<Document> listDatabases(ClientSession clientSession) {
        return delegatedMongoClient.listDatabases(clientSession);
    }

    @Override
    public <T> ListDatabasesPublisher<T> listDatabases(ClientSession clientSession, Class<T> aClass) {
        return delegatedMongoClient.listDatabases(clientSession, aClass);
    }

    @Override
    public ChangeStreamPublisher<Document> watch() {
        return delegatedMongoClient.watch();
    }

    @Override
    public <T> ChangeStreamPublisher<T> watch(Class<T> aClass) {
        return delegatedMongoClient.watch(aClass);
    }

    @Override
    public ChangeStreamPublisher<Document> watch(List<? extends Bson> list) {
        return delegatedMongoClient.watch(list);
    }

    @Override
    public <T> ChangeStreamPublisher<T> watch(List<? extends Bson> list, Class<T> aClass) {
        return delegatedMongoClient.watch(list, aClass);
    }

    @Override
    public ChangeStreamPublisher<Document> watch(ClientSession clientSession) {
        return delegatedMongoClient.watch(clientSession);
    }

    @Override
    public <T> ChangeStreamPublisher<T> watch(ClientSession clientSession, Class<T> aClass) {
        return delegatedMongoClient.watch(clientSession, aClass);
    }

    @Override
    public ChangeStreamPublisher<Document> watch(ClientSession clientSession, List<? extends Bson> list) {
        return delegatedMongoClient.watch(clientSession, list);
    }

    @Override
    public <T> ChangeStreamPublisher<T> watch(ClientSession clientSession, List<? extends Bson> list, Class<T> aClass) {
        return delegatedMongoClient.watch(clientSession, list, aClass);
    }

    @Override
    public Publisher<ClientSession> startSession() {
        return delegatedMongoClient.startSession();
    }

    @Override
    public Publisher<ClientSession> startSession(ClientSessionOptions clientSessionOptions) {
        return delegatedMongoClient.startSession(clientSessionOptions);
    }

    @Override
    public ClusterDescription getClusterDescription() {
        return delegatedMongoClient.getClusterDescription();
    }
}
