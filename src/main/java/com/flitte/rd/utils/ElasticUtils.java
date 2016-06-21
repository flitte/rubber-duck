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

package com.flitte.rd.utils;

import com.flitte.rd.ElasticStore;

/**
 * @author flitte
 * @since 20/06/2016.
 */
public final class ElasticUtils {

    /**
     * Default private constructor to prevent instantiation.
     */
    private ElasticUtils() {
        // Empty
    }

    public static void ensureIndexExists(final ElasticStore store) {
        // check for index, create if it is missing
    }

}
