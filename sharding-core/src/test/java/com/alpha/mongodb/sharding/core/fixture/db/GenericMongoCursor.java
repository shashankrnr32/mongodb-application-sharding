package com.alpha.mongodb.sharding.core.fixture.db;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoCursor;

import java.util.Iterator;
import java.util.List;

public class GenericMongoCursor<T> implements MongoCursor<T> {

    private final List<T> cursorObjectsList;
    private final Iterator<T> cursorObjectsIterator;

    public GenericMongoCursor(List<T> cursorObjectsList) {
        this.cursorObjectsList = cursorObjectsList;
        cursorObjectsIterator = cursorObjectsList.iterator();
    }

    @Override
    public void close() {

    }

    @Override
    public boolean hasNext() {
        return cursorObjectsIterator.hasNext();
    }

    @Override
    public T next() {
        return cursorObjectsIterator.next();
    }

    @Override
    public int available() {
        return 0;
    }

    @Override
    public T tryNext() {
        return cursorObjectsIterator.next();
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
