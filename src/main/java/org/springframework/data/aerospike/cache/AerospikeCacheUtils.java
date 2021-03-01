package org.springframework.data.aerospike.cache;

public class AerospikeCacheUtils {

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
