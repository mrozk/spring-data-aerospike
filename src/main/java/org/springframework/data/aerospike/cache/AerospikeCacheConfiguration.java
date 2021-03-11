package org.springframework.data.aerospike.cache;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@AllArgsConstructor
public class AerospikeCacheConfiguration {
    protected static final String DEFAULT_NAMESPACE = "test";
    protected static final String DEFAULT_SET_NAME = "aerospike";
    protected static final int DEFAULT_EXPIRATION = -1;

    @Builder.Default
    private String namespace = DEFAULT_NAMESPACE;
    @Builder.Default
    private String set = DEFAULT_SET_NAME;
    @Builder.Default
    private int expirationInSeconds = DEFAULT_EXPIRATION;

    public AerospikeCacheConfiguration () {
        this.namespace = DEFAULT_NAMESPACE;
        this.set = DEFAULT_SET_NAME;
        this.expirationInSeconds = DEFAULT_EXPIRATION;
    }
}
