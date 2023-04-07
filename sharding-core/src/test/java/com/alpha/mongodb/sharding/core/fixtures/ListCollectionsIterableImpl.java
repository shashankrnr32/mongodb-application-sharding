package com.alpha.mongodb.sharding.core.fixtures;

import com.mongodb.Function;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import org.bson.conversions.Bson;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ListCollectionsIterableImpl implements ListCollectionsIterable<String> {

    private final List<String> collectionNames;
    private final MongoCursor<String> mongoCursor;

    public ListCollectionsIterableImpl(List<String> collectionNames) {
        this.collectionNames = collectionNames;
        this.mongoCursor = new ListCollectionsIterator(collectionNames);
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

    public static class ListCollectionsIterator implements MongoCursor<String> {

        private final Iterator<String> collectionListIterator;

        public ListCollectionsIterator(final List<String> collectionList) {
            this.collectionListIterator = collectionList.iterator();
        }

        @Override
        public void close() {
        }

        @Override
        public boolean hasNext() {
            return collectionListIterator.hasNext();
        }

        @Override
        public String next() {
            return collectionListIterator.next();
        }

        @Override
        public int available() {
            return 0;
        }

        @Override
        public String tryNext() {
            return collectionListIterator.next();
        }

        @Override
        public ServerCursor getServerCursor() {
            return null;
        }

        @Override
        public ServerAddress getServerAddress() {
            return null;
        }
    }
}
