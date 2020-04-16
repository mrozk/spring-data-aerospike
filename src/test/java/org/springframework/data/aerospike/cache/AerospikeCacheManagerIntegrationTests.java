package org.springframework.data.aerospike.cache;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;

import static org.assertj.core.api.Assertions.assertThat;

public class AerospikeCacheManagerIntegrationTests extends BaseBlockingIntegrationTests {

    private static final String KEY = "foo";
    private static final String VALUE = "bar";

    @Autowired
    AerospikeClient client;
    @Autowired
    CachingComponent cachingComponent;

    @AfterEach
    public void tearDown() {
        cachingComponent.reset();
        client.delete(null, new Key(getNameSpace(), AerospikeCacheManager.DEFAULT_SET_NAME, KEY));
    }

    @Test
    public void shouldCache() {
        CachedObject response1 = cachingComponent.cachingMethod(KEY);
        CachedObject response2 = cachingComponent.cachingMethod(KEY);

        assertThat(response1).isNotNull();
        assertThat(response1.getValue()).isEqualTo(VALUE);
        assertThat(response2).isNotNull();
        assertThat(response2.getValue()).isEqualTo(VALUE);
        assertThat(cachingComponent.getNoOfCalls()).isEqualTo(1);
    }

    @Test
    public void shouldEvictCache() {
        CachedObject response1 = cachingComponent.cachingMethod(KEY);
        cachingComponent.cacheEvictingMethod(KEY);
        CachedObject response2 = cachingComponent.cachingMethod(KEY);

        assertThat(response1).isNotNull();
        assertThat(response1.getValue()).isEqualTo(VALUE);
        assertThat(response2).isNotNull();
        assertThat(response2.getValue()).isEqualTo(VALUE);
        assertThat(cachingComponent.getNoOfCalls()).isEqualTo(2);
    }

    public static class CachingComponent {

        private int noOfCalls = 0;

        public void reset() {
            noOfCalls = 0;
        }

        @Cacheable("TEST")
        public CachedObject cachingMethod(String param) {
            noOfCalls++;
            return new CachedObject(VALUE);
        }

        @CacheEvict("TEST")
        public void cacheEvictingMethod(String param) {

        }

        public int getNoOfCalls() {
            return noOfCalls;
        }
    }

    public static class CachedObject {
        private String value;

        public CachedObject(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

}
