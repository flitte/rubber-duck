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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flitte.rd.operation.handler.AddElementsHandler;
import com.flitte.rd.operation.handler.GetAdjacentEntitySeedsHandler;
import com.flitte.rd.operation.handler.GetAllElementsHandler;
import com.flitte.rd.operation.handler.GetElementsHandler;
import com.flitte.rd.utils.ElasticUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.operation.Operation;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import gaffer.operation.impl.get.GetAllElements;
import gaffer.operation.impl.get.GetElements;
import gaffer.store.Store;
import gaffer.store.StoreTrait;
import gaffer.store.operation.handler.OperationHandler;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static gaffer.store.StoreTrait.*;
import static java.net.InetAddress.getByName;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableSet;

/**
 * @author flitte
 * @since 18/06/16.
 */
public class ElasticStore extends Store {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticStore.class);
    private static final Set<StoreTrait> TRAITS = new HashSet<>(asList(FILTERING, TRANSFORMATION, STORE_VALIDATION));

    private Client client;

    @Override
    public Set<StoreTrait> getTraits() {
        return unmodifiableSet(TRAITS);
    }

    @Override
    protected boolean isValidationRequired() {
        return false;
    }

    public Client getClient() {
        if (client == null) {
            try {
                client = TransportClient.builder()
                                        .build()
                                        .addTransportAddress(
                                                // Replace with properties
                                                new InetSocketTransportAddress(getByName("localhost"), 9300));

            } catch (final UnknownHostException uhe) {
                LOGGER.error("Unable to connect to host", uhe);
                throw new ElasticsearchException(uhe);
            }
        }

        return client;
    }

    @Override
    protected void addAdditionalOperationHandlers() {
        // Empty
    }

    @Override
    protected OperationHandler<GetElements<ElementSeed, Element>, Iterable<Element>> getGetElementsHandler() {
        return new GetElementsHandler();
    }

    @Override
    protected OperationHandler<GetAllElements<Element>, Iterable<Element>> getGetAllElementsHandler() {
        return new GetAllElementsHandler();
    }

    @Override
    protected OperationHandler<? extends GetAdjacentEntitySeeds, Iterable<EntitySeed>> getAdjacentEntitySeedsHandler() {
        return new GetAdjacentEntitySeedsHandler();
    }

    @Override
    protected OperationHandler<? extends AddElements, Void> getAddElementsHandler() {
        return new AddElementsHandler();
    }

    @Override
    protected <OUTPUT> OUTPUT doUnhandledOperation(final Operation<?, OUTPUT> operation) {
        return null;
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE", justification = "The properties should always be ElasticProperties")
    public ElasticProperties getProperties() {
        return (ElasticProperties) super.getProperties();
    }

    public void addElements(final Iterable<Element> elements) {
        // Empty
        ElasticUtils.ensureIndexExists(this);
        insertGraphElements(elements);
    }

    protected void insertGraphElements(final Iterable<Element> elements) {

        final BulkRequestBuilder request = getClient().prepareBulk();
        final ObjectMapper mapper = new ObjectMapper();

        // Represent each element as a document, then pass on to elastic as the relevant type
        for (final Element element : elements) {
            if (element instanceof Edge) {
                // add new edge
                try {
                    request.add(getClient().prepareIndex(getProperties().getIndex(), "edge")
                                           .setSource(mapper.writeValueAsBytes(element)));
                } catch (final JsonProcessingException jpe) {
                    LOGGER.error("Failed to serialise edge for indexing: " + element);
                }
            } else if (element instanceof Entity) {
                // add new entity
                try {
                    request.add(getClient().prepareIndex(getProperties().getIndex(), "entity")
                                           .setSource(mapper.writeValueAsBytes(element)));
                } catch (final JsonProcessingException jpe) {
                    LOGGER.error("Failed to serialise edge for indexing: " + element);
                }
            } else {
                LOGGER.error("Failed to add element of type " + element.getGroup()
                        + " when attempting to insert elements.");
            }
        }

        final BulkResponse response = request.execute()
                                             .actionGet();
        if (!response.hasFailures()) {
            LOGGER.debug(String.format("Uploaded %d items in %s ms", response.getItems().length,
                    response.getTookInMillis()));
        } else {
            LOGGER.warn("Elastic indexing completed with failures.");
        }
    }

}
