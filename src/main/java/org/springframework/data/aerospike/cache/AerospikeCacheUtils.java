/*
 * Copyright 2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.cache;

public final class AerospikeCacheUtils {

    private AerospikeCacheUtils() {
    }

    /**
        Gets a cache name (an Aerospike namespace) and a set name and
        returns a combination that represents a full Aerospike cache name.

        @param cacheName a cache name (an Aerospike namespace)
        @param setName an Aerospike set name
        @return a full Aerospike cache name - combination of a given cache name (an Aerospike namespace)
        and a set name.
     */
    public static String getFullAerospikeCacheName(String cacheName, String setName) {
        return cacheName + ":" + setName;
    }
}
