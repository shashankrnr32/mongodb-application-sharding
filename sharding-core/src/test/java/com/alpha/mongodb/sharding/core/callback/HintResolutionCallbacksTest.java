package com.alpha.mongodb.sharding.core.callback;

import com.alpha.mongodb.sharding.core.fixtures.TestEntity1;
import com.alpha.mongodb.sharding.core.fixtures.TestEntity2;
import org.bson.Document;
import org.junit.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.query.BasicUpdate;
import org.springframework.data.mongodb.core.query.Query;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HintResolutionCallbacksTest {

    @Test
    public void testGetCallbackWhenCallbackDiscovered() {
        HintResolutionCallback<TestEntity1> testEntityHintResolutionCallback =
                new TestEntity1.TestEntity1HintResolutionCallback();
        HintResolutionCallbacks hintResolutionCallbacks = new HintResolutionCallbacks(Collections.singleton(testEntityHintResolutionCallback));
        Optional<HintResolutionCallback<TestEntity1>> callbackFromClassContext = hintResolutionCallbacks.getCallback(TestEntity1.class);
        assertTrue(callbackFromClassContext.isPresent());
    }

    @Test
    public void testGetCallbackWhenCallbackNotDiscovered() {
        HintResolutionCallbacks hintResolutionCallbacks = new HintResolutionCallbacks();
        Optional<HintResolutionCallback<TestEntity1>> callbackFromClassContext = hintResolutionCallbacks.getCallback(TestEntity1.class);
        assertFalse(callbackFromClassContext.isPresent());

        assertFalse(hintResolutionCallbacks.callbackForFindContext(TestEntity1.class, new Query()).isPresent());
        assertFalse(hintResolutionCallbacks.callbackForFindContext(TestEntity1.class, new Document()).isPresent());
        assertFalse(hintResolutionCallbacks.callbackForSaveContext(TestEntity1.class, new TestEntity1()).isPresent());
        assertFalse(hintResolutionCallbacks.callbackForUpdateContext(TestEntity1.class, new Query(), new BasicUpdate(new Document())).isPresent());
        assertFalse(hintResolutionCallbacks.callbackForUpdateContext(TestEntity1.class, new Document(), new BasicUpdate(new Document())).isPresent());
        assertFalse(hintResolutionCallbacks.callbackForDeleteContext(TestEntity1.class, new Query()).isPresent());
        assertFalse(hintResolutionCallbacks.callbackForDeleteContext(TestEntity1.class, new Document()).isPresent());
        assertFalse(hintResolutionCallbacks.callbackForDeleteContext(new TestEntity1()).isPresent());
    }

    @Test
    public void testCallbacksWhenCallbackDiscovered() {
        HintResolutionCallback<TestEntity1> testEntityHintResolutionCallback =
                new TestEntity1.TestEntity1HintResolutionCallback();
        HintResolutionCallbacks hintResolutionCallbacks = new HintResolutionCallbacks();
        hintResolutionCallbacks.discover(testEntityHintResolutionCallback);
        assertEquals(String.valueOf(0), hintResolutionCallbacks.callbackForFindContext(TestEntity1.class, new Query()).get().getCollectionHint());
        assertEquals(String.valueOf(0), hintResolutionCallbacks.callbackForFindContext(TestEntity1.class, new Document()).get().getCollectionHint());
        assertEquals(String.valueOf(0), hintResolutionCallbacks.callbackForSaveContext(TestEntity1.class, new TestEntity1()).get().getCollectionHint());
        assertEquals(String.valueOf(0), hintResolutionCallbacks.callbackForUpdateContext(TestEntity1.class, new Query(), new BasicUpdate(new Document())).get().getCollectionHint());
        assertEquals(String.valueOf(0), hintResolutionCallbacks.callbackForUpdateContext(TestEntity1.class, new Document(), new BasicUpdate(new Document())).get().getCollectionHint());
        assertEquals(String.valueOf(0), hintResolutionCallbacks.callbackForDeleteContext(TestEntity1.class, new Query()).get().getCollectionHint());
        assertEquals(String.valueOf(0), hintResolutionCallbacks.callbackForDeleteContext(TestEntity1.class, new Document()).get().getCollectionHint());
        assertEquals(String.valueOf(0), hintResolutionCallbacks.callbackForDeleteContext(new TestEntity1()).get().getCollectionHint());
    }

    @Test
    public void testDiscoverWithBeanFactory() {
        HintResolutionCallback<TestEntity1> testEntityHintResolutionCallback =
                new TestEntity1.TestEntity1HintResolutionCallback();
        Set<HintResolutionCallback> s = new HashSet<>();
        s.add(testEntityHintResolutionCallback);

        ApplicationContext context = mock(ApplicationContext.class);
        ObjectProvider<HintResolutionCallback> objectProvider = mock(ObjectProvider.class);
        when(context.getBeanProvider(eq(HintResolutionCallback.class))).thenReturn(objectProvider);
        when(objectProvider.stream()).thenReturn(s.stream());

        HintResolutionCallbacks hintResolutionCallbacks = new HintResolutionCallbacks();
        hintResolutionCallbacks.discover(context);

        Optional<HintResolutionCallback<TestEntity1>> callbackFromClassContext = hintResolutionCallbacks.getCallback(TestEntity1.class);
        assertTrue(callbackFromClassContext.isPresent());
    }

    @Test
    public void testGetCallbackWhenCallbackDiscoveredForAnotherEntity() {
        HintResolutionCallback<TestEntity1> testEntityHintResolutionCallback =
                new TestEntity1.TestEntity1HintResolutionCallback();
        HintResolutionCallbacks hintResolutionCallbacks = new HintResolutionCallbacks(Collections.singleton(testEntityHintResolutionCallback));
        Optional<HintResolutionCallback<TestEntity2>> callbackFromClassContext = hintResolutionCallbacks.getCallback(TestEntity2.class);
        assertFalse(callbackFromClassContext.isPresent());
    }


}