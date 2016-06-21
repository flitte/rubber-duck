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

import com.flitte.flubber.MockElastic;
import gaffer.commonutil.StreamUtil;
import gaffer.operation.Operation;
import gaffer.store.StoreException;
import gaffer.store.operation.handler.OperationHandler;
import gaffer.store.schema.Schema;
import org.elasticsearch.client.Client;

import static java.util.Collections.singletonList;

/**
 * @author flitte
 * @since 18/06/16.
 */
public class MockElasticStoreForTest extends ElasticStore {

    private MockElastic store = new MockElastic("data", singletonList("default_index"));

    public MockElasticStoreForTest() {
        final Schema schema = Schema.fromJson(
                StreamUtil.dataSchema(getClass()),
                StreamUtil.dataTypes(getClass()));

        final ElasticProperties properties = ElasticProperties.loadStoreProperties(StreamUtil.storeProps(getClass()));

        try {
            initialise(schema, properties);
        } catch (final StoreException e) {
            throw new RuntimeException(e);
        }
    }

    OperationHandler getOperationHandlerExposed(final Class<? extends Operation> opClass) {
        return super.getOperationHandler(opClass);
    }

    @Override
    public Client getClient() {
        return store.getClient();
    }

}
