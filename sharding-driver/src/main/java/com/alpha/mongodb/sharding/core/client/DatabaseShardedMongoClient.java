package com.alpha.mongodb.sharding.core.client;

import com.alpha.mongodb.sharding.core.configuration.DatabaseShardingOptions;
import com.alpha.mongodb.sharding.core.exception.UnresolvableDatabaseShardException;
import com.alpha.mongodb.sharding.core.hint.ShardingHint;
import com.alpha.mongodb.sharding.core.hint.ShardingHintManager;
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
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Database Sharded Mongo Client to get the databases which are sharded
 * by database.
 *
 * @author Shashank Sharma
 */
public class DatabaseShardedMongoClient implements MongoClient {

    private final DatabaseShardingOptions shardingOptions;

    private final Map<String, MongoClient> delegatedMongoClientMap;

    public DatabaseShardedMongoClient(Map<String, MongoClient> delegatedMongoClientMap,
                                      DatabaseShardingOptions shardingOptions) {
        this.shardingOptions = shardingOptions;
        this.delegatedMongoClientMap = delegatedMongoClientMap;
    }

    public DatabaseShardedMongoClient(MongoClient delegatedMongoClient, DatabaseShardingOptions shardingOptions) {
        this(shardingOptions.getDefaultDatabaseHintsSet().stream().collect(
                Collectors.toMap(s -> s, s -> delegatedMongoClient)), shardingOptions);
    }

    @Override
    public MongoDatabase getDatabase(String s) {
        return getDelegatedMongoClient().getDatabase(shardingOptions.resolveDatabaseName(s, resolveDatabaseHint()));
    }

    @Override
    public ClientSession startSession() {
        return getDelegatedMongoClient().startSession();
    }

    @Override
    public ClientSession startSession(ClientSessionOptions clientSessionOptions) {
        return getDelegatedMongoClient().startSession(clientSessionOptions);
    }

    @Override
    public void close() {
        getDelegatedMongoClient().close();
    }

    @Override
    public MongoIterable<String> listDatabaseNames() {
        return getDelegatedMongoClient().listDatabaseNames();
    }

    @Override
    public MongoIterable<String> listDatabaseNames(ClientSession clientSession) {
        return getDelegatedMongoClient().listDatabaseNames(clientSession);
    }

    @Override
    public ListDatabasesIterable<Document> listDatabases() {
        return getDelegatedMongoClient().listDatabases();
    }

    @Override
    public ListDatabasesIterable<Document> listDatabases(ClientSession clientSession) {
        return getDelegatedMongoClient().listDatabases(clientSession);
    }

    @Override
    public <T> ListDatabasesIterable<T> listDatabases(Class<T> aClass) {
        return getDelegatedMongoClient().listDatabases(aClass);
    }

    @Override
    public <T> ListDatabasesIterable<T> listDatabases(ClientSession clientSession, Class<T> aClass) {
        return getDelegatedMongoClient().listDatabases(clientSession, aClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch() {
        return getDelegatedMongoClient().watch();
    }

    @Override
    public <T> ChangeStreamIterable<T> watch(Class<T> aClass) {
        return getDelegatedMongoClient().watch(aClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch(List<? extends Bson> list) {
        return getDelegatedMongoClient().watch(list);
    }

    @Override
    public <T> ChangeStreamIterable<T> watch(List<? extends Bson> list, Class<T> aClass) {
        return getDelegatedMongoClient().watch(list, aClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch(ClientSession clientSession) {
        return getDelegatedMongoClient().watch(clientSession);
    }

    @Override
    public <T> ChangeStreamIterable<T> watch(ClientSession clientSession, Class<T> aClass) {
        return getDelegatedMongoClient().watch(clientSession, aClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch(ClientSession clientSession, List<? extends Bson> list) {
        return getDelegatedMongoClient().watch(clientSession, list);
    }

    @Override
    public <T> ChangeStreamIterable<T> watch(ClientSession clientSession, List<? extends Bson> list, Class<T> aClass) {
        return getDelegatedMongoClient().watch(clientSession, list, aClass);
    }

    @Override
    public ClusterDescription getClusterDescription() {
        return getDelegatedMongoClient().getClusterDescription();
    }

    private String resolveDatabaseHint() {
        return ShardingHintManager.getHint().map(ShardingHint::getDatabaseHint).orElseThrow(UnresolvableDatabaseShardException::new);
    }

    private MongoClient getDelegatedMongoClient() {
        String hint = resolveDatabaseHint();
        return Optional.ofNullable(delegatedMongoClientMap.get(hint)).orElseThrow(UnresolvableDatabaseShardException::new);
    }
}
