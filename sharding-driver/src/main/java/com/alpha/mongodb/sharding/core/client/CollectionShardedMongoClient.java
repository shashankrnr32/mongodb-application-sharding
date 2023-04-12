package com.alpha.mongodb.sharding.core.client;

import com.alpha.mongodb.sharding.core.configuration.CollectionShardingOptions;
import com.alpha.mongodb.sharding.core.database.CollectionShardedMongoDatabase;
import com.mongodb.ClientSessionOptions;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.connection.ClusterDescription;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;

/**
 * Collection Sharded Mongo Client that is used to access databases that
 * are sharded by the collection.
 *
 * @author Shashank Sharma
 */
public class CollectionShardedMongoClient implements ShardedMongoClient {
    private final CollectionShardingOptions shardingOptions;

    private final MongoClient delegatedMongoClient;

    public CollectionShardedMongoClient(MongoClient mongoClient,
                                        CollectionShardingOptions shardingOptions) {
        super();
        this.shardingOptions = shardingOptions;
        this.delegatedMongoClient = mongoClient;
    }

    @Override
    public MongoDatabase getDatabase(String s) {
        return new CollectionShardedMongoDatabase(delegatedMongoClient.getDatabase(s), shardingOptions);
    }

    @Override
    public ClientSession startSession() {
        return delegatedMongoClient.startSession();
    }

    @Override
    public ClientSession startSession(ClientSessionOptions clientSessionOptions) {
        return delegatedMongoClient.startSession(clientSessionOptions);
    }

    @Override
    public void close() {
        delegatedMongoClient.close();
    }

    @Override
    public MongoIterable<String> listDatabaseNames() {
        return delegatedMongoClient.listDatabaseNames();
    }

    @Override
    public MongoIterable<String> listDatabaseNames(ClientSession clientSession) {
        return delegatedMongoClient.listDatabaseNames(clientSession);
    }

    @Override
    public ListDatabasesIterable<Document> listDatabases() {
        return delegatedMongoClient.listDatabases();
    }

    @Override
    public ListDatabasesIterable<Document> listDatabases(ClientSession clientSession) {
        return delegatedMongoClient.listDatabases(clientSession);
    }

    @Override
    public <T> ListDatabasesIterable<T> listDatabases(Class<T> aClass) {
        return delegatedMongoClient.listDatabases(aClass);
    }

    @Override
    public <T> ListDatabasesIterable<T> listDatabases(ClientSession clientSession, Class<T> aClass) {
        return delegatedMongoClient.listDatabases(clientSession, aClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch() {
        return delegatedMongoClient.watch();
    }

    @Override
    public <T> ChangeStreamIterable<T> watch(Class<T> aClass) {
        return delegatedMongoClient.watch(aClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch(List<? extends Bson> list) {
        return delegatedMongoClient.watch(list);
    }

    @Override
    public <T> ChangeStreamIterable<T> watch(List<? extends Bson> list, Class<T> aClass) {
        return delegatedMongoClient.watch(list, aClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch(ClientSession clientSession) {
        return delegatedMongoClient.watch(clientSession);
    }

    @Override
    public <T> ChangeStreamIterable<T> watch(ClientSession clientSession, Class<T> aClass) {
        return delegatedMongoClient.watch(clientSession, aClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch(ClientSession clientSession, List<? extends Bson> list) {
        return delegatedMongoClient.watch(clientSession, list);
    }

    @Override
    public <T> ChangeStreamIterable<T> watch(ClientSession clientSession, List<? extends Bson> list, Class<T> aClass) {
        return delegatedMongoClient.watch(clientSession, list, aClass);
    }

    @Override
    public ClusterDescription getClusterDescription() {
        return delegatedMongoClient.getClusterDescription();
    }
}
