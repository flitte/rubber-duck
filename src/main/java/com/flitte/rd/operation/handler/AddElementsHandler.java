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
import gaffer.operation.OperationException;
import gaffer.operation.impl.add.AddElements;
import gaffer.store.Store;
import gaffer.store.operation.handler.OperationHandler;
import gaffer.user.User;

/**
 * @author flitte
 * @since 18/06/16.
 */
public class AddElementsHandler implements OperationHandler<AddElements, Void> {

    @Override
    public Void doOperation(final AddElements operation, final User user, final Store store) throws OperationException {
        addElements(operation, (ElasticStore) store);
        return null;
    }

    private void addElements(final AddElements operation, final ElasticStore store) {
        store.addElements(operation.getElements());
    }

}
