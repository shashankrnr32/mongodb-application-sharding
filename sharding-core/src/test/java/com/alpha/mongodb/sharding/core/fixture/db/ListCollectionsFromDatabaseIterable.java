package com.alpha.mongodb.sharding.core.fixture.db;

import com.mongodb.Function;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import org.bson.conversions.Bson;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ListCollectionsFromDatabaseIterable implements ListCollectionsIterable<String> {

    private final List<String> collectionNames;
    private final GenericMongoCursor<String> mongoCursor;

    public ListCollectionsFromDatabaseIterable(List<String> collectionNames) {
        this.collectionNames = collectionNames;
        this.mongoCursor = new GenericMongoCursor<>(collectionNames);
    }

    @Override
    public ListCollectionsIterable<String> filter(Bson bson) {
        return this;
    }

    @Override
    public ListCollectionsIterable<String> maxTime(long l, TimeUnit timeUnit) {
        return this;
    }

    @Override
    public MongoCursor<String> iterator() {
        return this.mongoCursor;
    }

    @Override
    public MongoCursor<String> cursor() {
        return this.mongoCursor;
    }

    @Override
    public String first() {
        return collectionNames.get(0);
    }

    @Override
    public <U> MongoIterable<U> map(Function<String, U> function) {
        return null;
    }

    @Override
    public <A extends Collection<? super String>> A into(A objects) {
        return null;
    }

    @Override
    public ListCollectionsIterable<String> batchSize(int i) {
        return this;
    }
}
