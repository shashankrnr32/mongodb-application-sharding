package com.alpha.mongodb.sharding.core.client;

import com.alpha.mongodb.sharding.core.configuration.DatabaseShardingOptions;
import com.alpha.mongodb.sharding.core.exception.UnresolvableDatabaseShardException;
import com.alpha.mongodb.sharding.core.hint.ShardingHint;
import com.alpha.mongodb.sharding.core.hint.ShardingHintManager;
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
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Database Sharded Reactive Mongo Client to get the databases which are sharded
 * by database.
 *
 * @author Shashank Sharma
 */
public class DatabaseShardedReactiveMongoClient implements ShardedReactiveMongoClient {

    private final DatabaseShardingOptions shardingOptions;

    private final Map<String, MongoClient> delegatedMongoClientMap;

    public DatabaseShardedReactiveMongoClient(Map<String, MongoClient> delegatedMongoClientMap,
                                              DatabaseShardingOptions shardingOptions) {
        this.shardingOptions = shardingOptions;
        this.delegatedMongoClientMap = delegatedMongoClientMap;
    }

    @Override
    public MongoDatabase getDatabase(String s) {
        return getDelegatedMongoClient().getDatabase(shardingOptions.resolveDatabaseName(s, resolveDatabaseHint()));

    }

    @Override
    public void close() {
        getDelegatedMongoClient().close();
    }

    @Override
    public Publisher<String> listDatabaseNames() {
        return getDelegatedMongoClient().listDatabaseNames();
    }

    @Override
    public Publisher<String> listDatabaseNames(ClientSession clientSession) {
        return getDelegatedMongoClient().listDatabaseNames(clientSession);
    }

    @Override
    public ListDatabasesPublisher<Document> listDatabases() {
        return getDelegatedMongoClient().listDatabases();
    }

    @Override
    public <T> ListDatabasesPublisher<T> listDatabases(Class<T> aClass) {
        return getDelegatedMongoClient().listDatabases(aClass);
    }

    @Override
    public ListDatabasesPublisher<Document> listDatabases(ClientSession clientSession) {
        return getDelegatedMongoClient().listDatabases(clientSession);
    }

    @Override
    public <T> ListDatabasesPublisher<T> listDatabases(ClientSession clientSession, Class<T> aClass) {
        return getDelegatedMongoClient().listDatabases(clientSession, aClass);
    }

    @Override
    public ChangeStreamPublisher<Document> watch() {
        return getDelegatedMongoClient().watch();
    }

    @Override
    public <T> ChangeStreamPublisher<T> watch(Class<T> aClass) {
        return getDelegatedMongoClient().watch(aClass);
    }

    @Override
    public ChangeStreamPublisher<Document> watch(List<? extends Bson> list) {
        return getDelegatedMongoClient().watch(list);
    }

    @Override
    public <T> ChangeStreamPublisher<T> watch(List<? extends Bson> list, Class<T> aClass) {
        return getDelegatedMongoClient().watch(list, aClass);
    }

    @Override
    public ChangeStreamPublisher<Document> watch(ClientSession clientSession) {
        return getDelegatedMongoClient().watch(clientSession);
    }

    @Override
    public <T> ChangeStreamPublisher<T> watch(ClientSession clientSession, Class<T> aClass) {
        return getDelegatedMongoClient().watch(clientSession, aClass);
    }

    @Override
    public ChangeStreamPublisher<Document> watch(ClientSession clientSession, List<? extends Bson> list) {
        return getDelegatedMongoClient().watch(clientSession, list);
    }

    @Override
    public <T> ChangeStreamPublisher<T> watch(ClientSession clientSession, List<? extends Bson> list, Class<T> aClass) {
        return getDelegatedMongoClient().watch(clientSession, list, aClass);
    }

    @Override
    public Publisher<ClientSession> startSession() {
        return getDelegatedMongoClient().startSession();
    }

    @Override
    public Publisher<ClientSession> startSession(ClientSessionOptions clientSessionOptions) {
        return getDelegatedMongoClient().startSession(clientSessionOptions);
    }

    @Override
    public ClusterDescription getClusterDescription() {
        return getDelegatedMongoClient().getClusterDescription();
    }

    public DatabaseShardedReactiveMongoClient(MongoClient delegatedMongoClient, DatabaseShardingOptions shardingOptions) {
        this(shardingOptions.getDefaultDatabaseHintsSet().stream().collect(
                Collectors.toMap(s -> s, s -> delegatedMongoClient)), shardingOptions);
    }

    private String resolveDatabaseHint() {
        return ShardingHintManager.getHint().map(ShardingHint::getDatabaseHint).orElseThrow(UnresolvableDatabaseShardException::new);
    }

    private MongoClient getDelegatedMongoClient() {
        String hint = resolveDatabaseHint();
        return Optional.ofNullable(delegatedMongoClientMap.get(hint)).orElseThrow(UnresolvableDatabaseShardException::new);
    }
}
