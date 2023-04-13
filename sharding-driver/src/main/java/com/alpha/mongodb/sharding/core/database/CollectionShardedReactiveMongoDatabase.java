package com.alpha.mongodb.sharding.core.database;

import com.alpha.mongodb.sharding.core.configuration.CollectionShardingOptions;
import com.alpha.mongodb.sharding.core.exception.UnresolvableCollectionShardException;
import com.alpha.mongodb.sharding.core.hint.ShardingHintManager;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateViewOptions;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.ListCollectionsPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import java.util.List;

/**
 * CollectionShardedReactiveMongoDatabase to query for collections that are sharded
 * within a single database.
 *
 * @author Shashank Sharma
 */
public class CollectionShardedReactiveMongoDatabase implements MongoDatabase {

    private final MongoDatabase delegatedMongoDatabase;
    private final CollectionShardingOptions shardingOptions;

    public CollectionShardedReactiveMongoDatabase(MongoDatabase delegatedMongoDatabase,
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
        return new CollectionShardedReactiveMongoDatabase(
                delegatedMongoDatabase.withCodecRegistry(codecRegistry), shardingOptions);
    }

    @Override
    public MongoDatabase withReadPreference(ReadPreference readPreference) {
        return new CollectionShardedReactiveMongoDatabase(
                delegatedMongoDatabase.withReadPreference(readPreference), shardingOptions);
    }

    @Override
    public MongoDatabase withWriteConcern(WriteConcern writeConcern) {
        return new CollectionShardedReactiveMongoDatabase(
                delegatedMongoDatabase.withWriteConcern(writeConcern), shardingOptions);
    }

    @Override
    public MongoDatabase withReadConcern(ReadConcern readConcern) {
        return new CollectionShardedReactiveMongoDatabase(
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
    public Publisher<Document> runCommand(Bson bson) {
        return delegatedMongoDatabase.runCommand(bson);
    }

    @Override
    public Publisher<Document> runCommand(Bson bson, ReadPreference readPreference) {
        return delegatedMongoDatabase.runCommand(bson, readPreference);
    }

    @Override
    public <T> Publisher<T> runCommand(Bson bson, Class<T> aClass) {
        return delegatedMongoDatabase.runCommand(bson, aClass);
    }

    @Override
    public <T> Publisher<T> runCommand(Bson bson, ReadPreference readPreference, Class<T> aClass) {
        return delegatedMongoDatabase.runCommand(bson, readPreference, aClass);
    }

    @Override
    public Publisher<Document> runCommand(ClientSession clientSession, Bson bson) {
        return delegatedMongoDatabase.runCommand(clientSession, bson);
    }

    @Override
    public Publisher<Document> runCommand(ClientSession clientSession, Bson bson, ReadPreference readPreference) {
        return delegatedMongoDatabase.runCommand(clientSession, bson, readPreference);
    }

    @Override
    public <T> Publisher<T> runCommand(ClientSession clientSession, Bson bson, Class<T> aClass) {
        return delegatedMongoDatabase.runCommand(clientSession, bson, aClass);
    }

    @Override
    public <T> Publisher<T> runCommand(ClientSession clientSession, Bson bson, ReadPreference readPreference, Class<T> aClass) {
        return delegatedMongoDatabase.runCommand(clientSession, bson, readPreference, aClass);
    }

    @Override
    public Publisher<Void> drop() {
        return delegatedMongoDatabase.drop();
    }

    @Override
    public Publisher<Void> drop(ClientSession clientSession) {
        return delegatedMongoDatabase.drop(clientSession);
    }

    @Override
    public Publisher<String> listCollectionNames() {
        return delegatedMongoDatabase.listCollectionNames();
    }

    @Override
    public Publisher<String> listCollectionNames(ClientSession clientSession) {
        return delegatedMongoDatabase.listCollectionNames();
    }

    @Override
    public ListCollectionsPublisher<Document> listCollections() {
        return delegatedMongoDatabase.listCollections();
    }

    @Override
    public <T> ListCollectionsPublisher<T> listCollections(Class<T> aClass) {
        return delegatedMongoDatabase.listCollections(aClass);
    }

    @Override
    public ListCollectionsPublisher<Document> listCollections(ClientSession clientSession) {
        return delegatedMongoDatabase.listCollections(clientSession);
    }

    @Override
    public <T> ListCollectionsPublisher<T> listCollections(ClientSession clientSession, Class<T> aClass) {
        return delegatedMongoDatabase.listCollections(clientSession, aClass);
    }

    @Override
    public Publisher<Void> createCollection(String s) {
        return delegatedMongoDatabase.createCollection(resolveCollectionName(s));
    }

    @Override
    public Publisher<Void> createCollection(String s, CreateCollectionOptions createCollectionOptions) {
        return delegatedMongoDatabase.createCollection(resolveCollectionName(s), createCollectionOptions);
    }

    @Override
    public Publisher<Void> createCollection(ClientSession clientSession, String s) {
        return delegatedMongoDatabase.createCollection(clientSession, resolveCollectionName(s));
    }

    @Override
    public Publisher<Void> createCollection(ClientSession clientSession, String s, CreateCollectionOptions createCollectionOptions) {
        return delegatedMongoDatabase.createCollection(clientSession, resolveCollectionName(s), createCollectionOptions);
    }

    @Override
    public Publisher<Void> createView(String s, String s1, List<? extends Bson> list) {
        return delegatedMongoDatabase.createView(s, s1, list);
    }

    @Override
    public Publisher<Void> createView(String s, String s1, List<? extends Bson> list, CreateViewOptions createViewOptions) {
        return delegatedMongoDatabase.createView(s, s1, list, createViewOptions);
    }

    @Override
    public Publisher<Void> createView(ClientSession clientSession, String s, String s1, List<? extends Bson> list) {
        return delegatedMongoDatabase.createView(clientSession, s, s1, list);
    }

    @Override
    public Publisher<Void> createView(ClientSession clientSession, String s, String s1, List<? extends Bson> list, CreateViewOptions createViewOptions) {
        return delegatedMongoDatabase.createView(clientSession, s, s1, list, createViewOptions);
    }

    @Override
    public ChangeStreamPublisher<Document> watch() {
        return delegatedMongoDatabase.watch();
    }

    @Override
    public <T> ChangeStreamPublisher<T> watch(Class<T> aClass) {
        return delegatedMongoDatabase.watch(aClass);
    }

    @Override
    public ChangeStreamPublisher<Document> watch(List<? extends Bson> list) {
        return delegatedMongoDatabase.watch(list);
    }

    @Override
    public <T> ChangeStreamPublisher<T> watch(List<? extends Bson> list, Class<T> aClass) {
        return delegatedMongoDatabase.watch(list, aClass);
    }

    @Override
    public ChangeStreamPublisher<Document> watch(ClientSession clientSession) {
        return delegatedMongoDatabase.watch(clientSession);
    }

    @Override
    public <T> ChangeStreamPublisher<T> watch(ClientSession clientSession, Class<T> aClass) {
        return delegatedMongoDatabase.watch(clientSession, aClass);
    }

    @Override
    public ChangeStreamPublisher<Document> watch(ClientSession clientSession, List<? extends Bson> list) {
        return delegatedMongoDatabase.watch(clientSession, list);
    }

    @Override
    public <T> ChangeStreamPublisher<T> watch(ClientSession clientSession, List<? extends Bson> list, Class<T> aClass) {
        return delegatedMongoDatabase.watch(clientSession, list, aClass);
    }

    @Override
    public AggregatePublisher<Document> aggregate(List<? extends Bson> list) {
        return delegatedMongoDatabase.aggregate(list);
    }

    @Override
    public <T> AggregatePublisher<T> aggregate(List<? extends Bson> list, Class<T> aClass) {
        return delegatedMongoDatabase.aggregate(list, aClass);
    }

    @Override
    public AggregatePublisher<Document> aggregate(ClientSession clientSession, List<? extends Bson> list) {
        return delegatedMongoDatabase.aggregate(clientSession, list);
    }

    @Override
    public <T> AggregatePublisher<T> aggregate(ClientSession clientSession, List<? extends Bson> list, Class<T> aClass) {
        return delegatedMongoDatabase.aggregate(clientSession, list, aClass);
    }

    private String resolveCollectionName(String s) throws UnresolvableCollectionShardException {
        return shardingOptions.resolveCollectionName(s, ShardingHintManager.getHint().map(shardingHint -> {
            shardingOptions.validateCollectionHint(s, shardingHint.getCollectionHint());
            return shardingHint.getCollectionHint();
        }).orElseThrow(UnresolvableCollectionShardException::new));
    }
}
