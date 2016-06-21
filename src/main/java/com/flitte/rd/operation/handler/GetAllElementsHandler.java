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

package com.flitte.rd.operation.handler;

import com.flitte.rd.ElasticStore;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.operation.OperationException;
import gaffer.operation.impl.get.GetAllElements;
import gaffer.store.Store;
import gaffer.store.operation.handler.OperationHandler;
import gaffer.user.User;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author flitte
 * @since 18/06/16.
 */
public class GetAllElementsHandler implements OperationHandler<GetAllElements<Element>, Iterable<Element>> {

    @Override
    public Iterable<Element> doOperation(final GetAllElements<Element> operation, final User user, final Store store)
            throws OperationException {
        return doOperation(operation, user, (ElasticStore) store);
    }

    public Iterable<Element> doOperation(final GetAllElements<Element> operation,
                                         final User user,
                                         final ElasticStore store) throws OperationException {

        final SearchHits hits = store.getClient()
                                     .prepareSearch()
                                     .execute()
                                     .actionGet()
                                     .getHits();

        final ObjectMapper mapper = new ObjectMapper();

        final List<Element> entities = new ArrayList<>();

        for (final SearchHit hit : hits) {

            try {
                entities.add(mapper.readValue(hit.sourceAsString(), Entity.class));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return entities;
    }
}
