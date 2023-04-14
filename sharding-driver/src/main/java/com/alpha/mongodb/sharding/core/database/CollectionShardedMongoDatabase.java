package com.alpha.mongodb.sharding.core.database;

import com.alpha.mongodb.sharding.core.configuration.CollectionShardingOptions;
import com.alpha.mongodb.sharding.core.exception.UnresolvableCollectionShardException;
import com.alpha.mongodb.sharding.core.hint.ShardingHintManager;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateViewOptions;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.List;

/**
 * CollectionShardedMongoDatabase to query for collections that are sharded
 * within a single database.
 *
 * @author Shashank Sharma
 */
public class CollectionShardedMongoDatabase implements MongoDatabase {

    private final MongoDatabase delegatedMongoDatabase;
    private final CollectionShardingOptions shardingOptions;

    public CollectionShardedMongoDatabase(MongoDatabase delegatedMongoDatabase,
                                          CollectionShardingOptions shardingOptions) {
        this.delegatedMongoDatabase = delegatedMongoDatabase;
        this.shardingOptions = shardingOptions;
    }

    @Override
    public String getName() {
        return delegatedMongoDatabase.getName();
    }

    @Override
    public CodecRegistry getCodecRegistry() {
        return delegatedMongoDatabase.getCodecRegistry();
    }

    @Override
    public ReadPreference getReadPreference() {
        return delegatedMongoDatabase.getReadPreference();
    }

    @Override
    public WriteConcern getWriteConcern() {
        return delegatedMongoDatabase.getWriteConcern();
    }

    @Override
    public ReadConcern getReadConcern() {
        return delegatedMongoDatabase.getReadConcern();
    }

    @Override
    public MongoDatabase withCodecRegistry(CodecRegistry codecRegistry) {
        return new CollectionShardedMongoDatabase(
                delegatedMongoDatabase.withCodecRegistry(codecRegistry), shardingOptions);
    }

    @Override
    public MongoDatabase withReadPreference(ReadPreference readPreference) {
        return new CollectionShardedMongoDatabase(
                delegatedMongoDatabase.withReadPreference(readPreference), shardingOptions);
    }

    @Override
    public MongoDatabase withWriteConcern(WriteConcern writeConcern) {
        return new CollectionShardedMongoDatabase(
                delegatedMongoDatabase.withWriteConcern(writeConcern), shardingOptions);
    }

    @Override
    public MongoDatabase withReadConcern(ReadConcern readConcern) {
        return new CollectionShardedMongoDatabase(
                delegatedMongoDatabase.withReadConcern(readConcern), shardingOptions);
    }

    @Override
    public MongoCollection<Document> getCollection(String s) {
        return delegatedMongoDatabase.getCollection(resolveCollectionName(s));
    }

    @Override
    public <T> MongoCollection<T> getCollection(String s, Class<T> aClass) {
        return delegatedMongoDatabase.getCollection(resolveCollectionName(s), aClass);
    }

    @Override
    public Document runCommand(Bson bson) {
        return delegatedMongoDatabase.runCommand(bson);
    }

    @Override
    public Document runCommand(Bson bson, ReadPreference readPreference) {
        return delegatedMongoDatabase.runCommand(bson, readPreference);
    }

    @Override
    public <T> T runCommand(Bson bson, Class<T> aClass) {
        return delegatedMongoDatabase.runCommand(bson, aClass);
    }

    @Override
    public <T> T runCommand(Bson bson, ReadPreference readPreference, Class<T> aClass) {
        return delegatedMongoDatabase.runCommand(bson, readPreference, aClass);
    }

    @Override
    public Document runCommand(ClientSession clientSession, Bson bson) {
        return delegatedMongoDatabase.runCommand(clientSession, bson);
    }

    @Override
    public Document runCommand(ClientSession clientSession, Bson bson, ReadPreference readPreference) {
        return delegatedMongoDatabase.runCommand(clientSession, bson, readPreference);
    }

    @Override
    public <T> T runCommand(ClientSession clientSession, Bson bson, Class<T> aClass) {
        return delegatedMongoDatabase.runCommand(clientSession, bson, aClass);
    }

    @Override
    public <T> T runCommand(ClientSession clientSession, Bson bson, ReadPreference readPreference, Class<T> aClass) {
        return delegatedMongoDatabase.runCommand(clientSession, bson, readPreference, aClass);
    }

    @Override
    public void drop() {
        delegatedMongoDatabase.drop();
    }

    @Override
    public void drop(ClientSession clientSession) {
        delegatedMongoDatabase.drop(clientSession);
    }

    @Override
    public MongoIterable<String> listCollectionNames() {
        return delegatedMongoDatabase.listCollectionNames();
    }

    @Override
    public ListCollectionsIterable<Document> listCollections() {
        return delegatedMongoDatabase.listCollections();
    }

    @Override
    public <T> ListCollectionsIterable<T> listCollections(Class<T> aClass) {
        return delegatedMongoDatabase.listCollections(aClass);
    }

    @Override
    public MongoIterable<String> listCollectionNames(ClientSession clientSession) {
        return delegatedMongoDatabase.listCollectionNames(clientSession);
    }

    @Override
    public ListCollectionsIterable<Document> listCollections(ClientSession clientSession) {
        return delegatedMongoDatabase.listCollections(clientSession);
    }

    @Override
    public <T> ListCollectionsIterable<T> listCollections(ClientSession clientSession, Class<T> aClass) {
        return delegatedMongoDatabase.listCollections(clientSession, aClass);
    }

    @Override
    public void createCollection(String s) {
        delegatedMongoDatabase.createCollection(resolveCollectionName(s));
    }

    @Override
    public void createCollection(String s, CreateCollectionOptions createCollectionOptions) {
        delegatedMongoDatabase.createCollection(resolveCollectionName(s), createCollectionOptions);
    }

    @Override
    public void createCollection(ClientSession clientSession, String s) {
        delegatedMongoDatabase.createCollection(clientSession, resolveCollectionName(s));
    }

    @Override
    public void createCollection(ClientSession clientSession, String s, CreateCollectionOptions createCollectionOptions) {
        delegatedMongoDatabase.createCollection(clientSession, resolveCollectionName(s), createCollectionOptions);
    }

    @Override
    public void createView(String s, String s1, List<? extends Bson> list) {
        delegatedMongoDatabase.createView(s, s1, list);
    }

    @Override
    public void createView(String s, String s1, List<? extends Bson> list, CreateViewOptions createViewOptions) {
        delegatedMongoDatabase.createView(s, s1, list, createViewOptions);
    }

    @Override
    public void createView(ClientSession clientSession, String s, String s1, List<? extends Bson> list) {
        delegatedMongoDatabase.createView(clientSession, s, s1, list);
    }

    @Override
    public void createView(ClientSession clientSession, String s, String s1, List<? extends Bson> list, CreateViewOptions createViewOptions) {
        delegatedMongoDatabase.createView(clientSession, s, s1, list, createViewOptions);
    }

    @Override
    public ChangeStreamIterable<Document> watch() {
        return delegatedMongoDatabase.watch();
    }

    @Override
    public <T> ChangeStreamIterable<T> watch(Class<T> aClass) {
        return delegatedMongoDatabase.watch(aClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch(List<? extends Bson> list) {
        return delegatedMongoDatabase.watch(list);
    }

    @Override
    public <T> ChangeStreamIterable<T> watch(List<? extends Bson> list, Class<T> aClass) {
        return delegatedMongoDatabase.watch(list, aClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch(ClientSession clientSession) {
        return delegatedMongoDatabase.watch(clientSession);
    }

    @Override
    public <T> ChangeStreamIterable<T> watch(ClientSession clientSession, Class<T> aClass) {
        return delegatedMongoDatabase.watch(clientSession, aClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch(ClientSession clientSession, List<? extends Bson> list) {
        return delegatedMongoDatabase.watch(clientSession, list);
    }

    @Override
    public <T> ChangeStreamIterable<T> watch(ClientSession clientSession, List<? extends Bson> list, Class<T> aClass) {
        return delegatedMongoDatabase.watch(clientSession, list, aClass);
    }

    @Override
    public AggregateIterable<Document> aggregate(List<? extends Bson> list) {
        return delegatedMongoDatabase.aggregate(list);
    }

    @Override
    public <T> AggregateIterable<T> aggregate(List<? extends Bson> list, Class<T> aClass) {
        return delegatedMongoDatabase.aggregate(list, aClass);
    }

    @Override
    public AggregateIterable<Document> aggregate(ClientSession clientSession, List<? extends Bson> list) {
        return delegatedMongoDatabase.aggregate(clientSession, list);
    }

    @Override
    public <T> AggregateIterable<T> aggregate(ClientSession clientSession, List<? extends Bson> list, Class<T> aClass) {
        return delegatedMongoDatabase.aggregate(clientSession, list, aClass);
    }

    private String resolveCollectionName(String s) throws UnresolvableCollectionShardException {
        return shardingOptions.resolveCollectionName(s, ShardingHintManager.getHint().map(shardingHint -> {
            shardingOptions.validateCollectionHint(s, shardingHint.getCollectionHint());
            return shardingHint.getCollectionHint();
        }).orElseThrow(UnresolvableCollectionShardException::new));
    }
}
