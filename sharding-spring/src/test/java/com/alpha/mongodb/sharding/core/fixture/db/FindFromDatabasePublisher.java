package com.alpha.mongodb.sharding.core.fixture.db;

import com.mongodb.CursorType;
import com.mongodb.ExplainVerbosity;
import com.mongodb.client.model.Collation;
import com.mongodb.reactivestreams.client.FindPublisher;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class FindFromDatabasePublisher implements FindPublisher<Document> {

    private final List<Document> documents;
    private final GenericMongoCursor<Document> documentGenericMongoCursor;

    public FindFromDatabasePublisher(List<Document> documents) {
        this.documents = documents;
        this.documentGenericMongoCursor = new GenericMongoCursor<>(documents);
    }

    public FindFromDatabasePublisher(Document... documents) {
        this(Arrays.stream(documents).collect(Collectors.toList()));
    }


    @Override
    public Publisher<Document> first() {
        return Flux.just(documents.get(0));
    }

    @Override
    public FindPublisher<Document> filter(Bson bson) {
        return this;
    }

    @Override
    public FindPublisher<Document> limit(int i) {
        return this;
    }

    @Override
    public FindPublisher<Document> skip(int i) {
        return this;
    }

    @Override
    public FindPublisher<Document> maxTime(long l, TimeUnit timeUnit) {
        return this;
    }

    @Override
    public FindPublisher<Document> maxAwaitTime(long l, TimeUnit timeUnit) {
        return this;
    }

    @Override
    public FindPublisher<Document> projection(Bson bson) {
        return this;
    }

    @Override
    public FindPublisher<Document> sort(Bson bson) {
        return this;
    }

    @Override
    public FindPublisher<Document> noCursorTimeout(boolean b) {
        return this;
    }

    @Override
    public FindPublisher<Document> oplogReplay(boolean b) {
        return this;
    }

    @Override
    public FindPublisher<Document> partial(boolean b) {
        return this;
    }

    @Override
    public FindPublisher<Document> cursorType(CursorType cursorType) {
        return this;
    }

    @Override
    public FindPublisher<Document> collation(Collation collation) {
        return this;
    }

    @Override
    public FindPublisher<Document> comment(String s) {
        return this;
    }

    @Override
    public FindPublisher<Document> comment(BsonValue bsonValue) {
        return this;
    }

    @Override
    public FindPublisher<Document> hint(Bson bson) {
        return this;
    }

    @Override
    public FindPublisher<Document> hintString(String s) {
        return this;
    }

    @Override
    public FindPublisher<Document> let(Bson bson) {
        return this;
    }

    @Override
    public FindPublisher<Document> max(Bson bson) {
        return this;
    }

    @Override
    public FindPublisher<Document> min(Bson bson) {
        return this;
    }

    @Override
    public FindPublisher<Document> returnKey(boolean b) {
        return this;
    }

    @Override
    public FindPublisher<Document> showRecordId(boolean b) {
        return this;
    }

    @Override
    public FindPublisher<Document> batchSize(int i) {
        return this;
    }

    @Override
    public FindPublisher<Document> allowDiskUse(Boolean aBoolean) {
        return this;
    }

    @Override
    public Publisher<Document> explain() {
        return Flux.fromIterable(documents);
    }

    @Override
    public Publisher<Document> explain(ExplainVerbosity explainVerbosity) {
        return Flux.fromIterable(documents);
    }

    @Override
    public <E> Publisher<E> explain(Class<E> aClass) {
        return null;
    }

    @Override
    public <E> Publisher<E> explain(Class<E> aClass, ExplainVerbosity explainVerbosity) {
        return null;
    }

    @Override
    public void subscribe(Subscriber<? super Document> subscriber) {
        Flux.fromIterable(documents).subscribe(subscriber);
    }
}
