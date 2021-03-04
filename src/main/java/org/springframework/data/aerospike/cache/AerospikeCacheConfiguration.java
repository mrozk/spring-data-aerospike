package org.springframework.data.aerospike.cache;

import lombok.Builder;
import lombok.Data;
import reactor.util.annotation.Nullable;

@Data
@Builder
public class AerospikeCacheConfiguration {

    protected static final String DEFAULT_NAMESPACE = "test";
    protected static final String DEFAULT_SET_NAME = "aerospike";
    protected static final int DEFAULT_EXPIRATION = -1;

    private final String namespace;
    private final String set;
    private final int expirationInSeconds;

    public AerospikeCacheConfiguration() {
        this(DEFAULT_NAMESPACE, DEFAULT_SET_NAME, DEFAULT_EXPIRATION);
    }

    public AerospikeCacheConfiguration(@Nullable String namespace, @Nullable String set, int expirationInSeconds) {
        this.namespace = (namespace != null ? namespace : DEFAULT_NAMESPACE);
        this.set = (set != null ? set : DEFAULT_SET_NAME);
        this.expirationInSeconds = expirationInSeconds;
    }
}
