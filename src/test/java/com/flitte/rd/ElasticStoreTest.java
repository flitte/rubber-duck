/*
 * (C) Copyright 2016 flitte
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flitte.rd;

import com.flitte.rd.util.TestGroups;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.get.GetAllElements;
import gaffer.operation.impl.get.GetElements;
import gaffer.operation.impl.get.GetElementsSeed;
import gaffer.operation.impl.get.GetRelatedElements;
import gaffer.store.StoreException;
import gaffer.store.StoreTrait;
import gaffer.user.User;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.google.common.collect.Iterables.size;
import static gaffer.store.StoreTrait.*;
import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author flitte
 * @since 18/06/16.
 */
public class ElasticStoreTest {

    private MockElasticStoreForTest store;

    private final Entity e = new Entity(TestGroups.ENTITY);
    private final User user = new User();

    @Before
    public void setup() throws Exception {
        System.out.println("Starting new mock ES instance...");
        store = new MockElasticStoreForTest();

        insertElements();

        // Wait briefly to ensure documents are indexed...
        Thread.sleep(25000L);
    }

    @After
    public void tearDown() {
        System.out.println("Killing mock ES instance...");
        store = null;
    }

    @Test
    public void testAbleToGetAllElements() throws OperationException {
        final GetAllElements<Element> getAllElements = new GetAllElements.Builder<>().view(new View.Builder()
                                                                                                   .entity(TestGroups.ENTITY)
                                                                                                   .build())
                                                                                     .build();
        final Iterable<Element> elementResults = store.execute(getAllElements, user);

        final GetAllElements<Edge> getAllEdges = new GetAllElements.Builder<Edge>().view(new View.Builder()
                                                                                                 .entity(TestGroups.ENTITY)
                                                                                                 .build())
                                                                                   .build();
        final Iterable<Edge> edgeResults = store.execute(getAllEdges, user);

        final GetAllElements<Entity> getAllEntities = new GetAllElements.Builder<Entity>()
                .view(new View.Builder()
                              .entity(TestGroups.ENTITY)
                              .build())
                .build();

        final Iterable<Entity> entityResults = store.execute(getAllEntities, user);


        final EntitySeed entitySeed1 = new EntitySeed("1");

        final GetElements<EntitySeed, Element> getBySeed = new GetElementsSeed.Builder<EntitySeed, Element>()
                .view(new View.Builder()
                              .entity(TestGroups.ENTITY)
                              .build())
                .addSeed(entitySeed1)
                .build();
        final Iterable<Element> results = store.execute(getBySeed, user);

        assertThat(1, equalTo(size(results)));
        assertThat(results, hasItem(e));

        final GetRelatedElements<EntitySeed, Element> getRelated = new GetRelatedElements.Builder<EntitySeed, Element>()
                .view(new View.Builder()
                              .entity(TestGroups.ENTITY)
                              .build())
                .addSeed(entitySeed1)
                .build();
        final Iterable<Element> relatedResults = store.execute(getRelated, user);
        assertEquals(1, size(relatedResults));
        assertThat(relatedResults, hasItem(e));
    }


    @Test
    public void testStoreReturnsHandlersForRegisteredOperations() throws StoreException {
        // Then
//        assertNotNull(store.getOperationHandlerExposed(Validate.class));
//
//        assertTrue(store.getOperationHandlerExposed(AddElementsFromHdfs.class) instanceof AddElementsFromHdfsHandler);
//        assertTrue(
//                store.getOperationHandlerExposed(GetEdgesBetweenSets.class) instanceof GetElementsBetweenSetsHandler);
//        assertTrue(store.getOperationHandlerExposed(
//                GetElementsBetweenSets.class) instanceof GetElementsBetweenSetsHandler);
//        assertTrue(store.getOperationHandlerExposed(GetElementsInRanges.class) instanceof GetElementsInRangesHandler);
//        assertTrue(store.getOperationHandlerExposed(GetEdgesInRanges.class) instanceof GetElementsInRangesHandler);
//        assertTrue(store.getOperationHandlerExposed(GetEntitiesInRanges.class) instanceof GetElementsInRangesHandler);
//        assertTrue(store.getOperationHandlerExposed(GetElementsWithinSet.class) instanceof GetElementsWithinSetHandler);
//        assertTrue(store.getOperationHandlerExposed(GetEdgesWithinSet.class) instanceof GetElementsWithinSetHandler);
//        assertTrue(store.getOperationHandlerExposed(SplitTable.class) instanceof SplitTableHandler);
//        assertTrue(store.getOperationHandlerExposed(
//                SampleDataForSplitPoints.class) instanceof SampleDataForSplitPointsHandler);
//        assertTrue(store.getOperationHandlerExposed(
//                ImportAccumuloKeyValueFiles.class) instanceof ImportAccumuloKeyValueFilesHandler);
//        assertTrue(store.getOperationHandlerExposed(GenerateElements.class) instanceof GenerateElementsHandler);
//        assertTrue(store.getOperationHandlerExposed(GenerateObjects.class) instanceof GenerateObjectsHandler);
    }

    @Test
    public void testRequestForNullHandlerManaged() {
//        final OperationHandler returnedHandler = store.getOperationHandlerExposed(null);
//        assertNull(returnedHandler);
    }

    @Test
    public void testStoreTraits() {
        final Collection<StoreTrait> traits = store.getTraits();
        assertNotNull(traits);
        assertTrue("Collection size should be 4", traits.size() == 4);
        assertTrue("Collection should contain FILTERING trait", traits.contains(FILTERING));
        assertTrue("Collection should contain TRANSFORMATION trait", traits.contains(TRANSFORMATION));
        assertTrue("Collection should contain STORE_VALIDATION trait", traits.contains(STORE_VALIDATION));
    }

    private void insertElements() throws OperationException {
        final List<Element> elements = new ArrayList<>();

        e.setVertex("1");
        elements.add(e);

        final AddElements add = new AddElements.Builder()
                .elements(elements)
                .build();

        store.execute(add, user);
    }

}
