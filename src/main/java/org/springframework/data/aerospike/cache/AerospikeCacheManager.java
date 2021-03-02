/*
 * Copyright 2019 the original author or authors.
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

import com.aerospike.client.IAerospikeClient;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.transaction.AbstractTransactionSupportingCacheManager;
import org.springframework.cache.transaction.TransactionAwareCacheDecorator;
import org.springframework.data.aerospike.convert.AerospikeConverter;
import org.springframework.util.Assert;

import java.util.*;

/**
 * {@link CacheManager} implementation for Aerospike. By default {@link AerospikeCache}s
 * will be lazily initialized for each {@link #getCache(String)} request unless a set of
 * predefined cache names is provided. <br>
 * <br>
 * Setting {@link #setTransactionAware(boolean)} to <code>true</code> will force Caches to
 * be decorated as {@link TransactionAwareCacheDecorator} so values will only be written
 * to the cache after successful commit of surrounding transaction.
 *
 * @author Venil Noronha
 */
public class AerospikeCacheManager extends AbstractTransactionSupportingCacheManager {

	protected static final String DEFAULT_SET_NAME = "aerospike";
	protected static final int DEFAULT_TIME_TO_LIVE = -1;

	private final IAerospikeClient aerospikeClient;
	private final AerospikeConverter aerospikeConverter;
	private final String setName;
	private final Set<String> configuredCacheNames;
	private final int defaultTimeToLive;

	/**
	 * Create a new {@link AerospikeCacheManager} instance -
	 * Without specifying any additional parameter.
	 *
	 * @param aerospikeClient the instance that implements {@link IAerospikeClient}.
	 * @param aerospikeConverter the instance that implements {@link AerospikeConverter}.
	 */
	public AerospikeCacheManager(IAerospikeClient aerospikeClient,
								 AerospikeConverter aerospikeConverter) {
		this(aerospikeClient, aerospikeConverter, Collections.emptyList(), DEFAULT_SET_NAME, DEFAULT_TIME_TO_LIVE);
	}

	/**
	 * Create a new {@link AerospikeCacheManager} instance -
	 * Specifying the set name.
	 *
	 * @param aerospikeClient the instance that implements {@link IAerospikeClient}.
	 * @param aerospikeConverter the instance that implements {@link AerospikeConverter}.
	 * @param setName the set name.
	 */
	public AerospikeCacheManager(IAerospikeClient aerospikeClient,
								 AerospikeConverter aerospikeConverter,
								 String setName) {
		this(aerospikeClient, aerospikeConverter, Collections.emptyList(), setName, DEFAULT_TIME_TO_LIVE);
	}

	/**
	 * Create a new {@link AerospikeCacheManager} instance -
	 * Specifying the cache names (Aerospike namespaces).
	 *
	 * @param aerospikeClient the instance that implements {@link IAerospikeClient}.
	 * @param aerospikeConverter the instance that implements {@link AerospikeConverter}.
	 * @param cacheNames the default caches to create.
	 */
	public AerospikeCacheManager(IAerospikeClient aerospikeClient,
								 AerospikeConverter aerospikeConverter,
								 Collection<String> cacheNames) {
		this(aerospikeClient, aerospikeConverter, cacheNames, DEFAULT_SET_NAME, DEFAULT_TIME_TO_LIVE);
	}

	/**
	 * Create a new {@link AerospikeCacheManager} instance -
	 * Specifying the time to live configuration.
	 *
	 * @param aerospikeClient the instance that implements {@link IAerospikeClient}.
	 * @param aerospikeConverter the instance that implements {@link AerospikeConverter}.
	 * @param defaultTimeToLive the time to live configuration.
	 */
	public AerospikeCacheManager(IAerospikeClient aerospikeClient,
								 AerospikeConverter aerospikeConverter,
								 int defaultTimeToLive) {
		this(aerospikeClient, aerospikeConverter, Collections.emptyList(), DEFAULT_SET_NAME, defaultTimeToLive);
	}

	/**
	 * Create a new {@link AerospikeCacheManager} instance -
	 * Specifying the cache names (Aerospike namespaces) and set name.
	 *
	 * @param aerospikeClient the instance that implements {@link IAerospikeClient}.
	 * @param aerospikeConverter the instance that implements {@link AerospikeConverter}.
	 * @param cacheNames the default caches to create.
	 * @param setName the set name.
	 */
	public AerospikeCacheManager(IAerospikeClient aerospikeClient,
								 AerospikeConverter aerospikeConverter,
								 Collection<String> cacheNames,
								 String setName) {
		this(aerospikeClient, aerospikeConverter, cacheNames, setName, DEFAULT_TIME_TO_LIVE);
	}

	/**
	 * Create a new {@link AerospikeCacheManager} instance -
	 * Specifying the cache names (Aerospike namespaces) and time to live configuration.
	 *
	 * @param aerospikeClient the instance that implements {@link IAerospikeClient}.
	 * @param aerospikeConverter the instance that implements {@link AerospikeConverter}.
	 * @param cacheNames the default caches to create.
	 * @param defaultTimeToLive the time to live configuration.
	 */
	public AerospikeCacheManager(IAerospikeClient aerospikeClient,
								 AerospikeConverter aerospikeConverter,
								 Collection<String> cacheNames,
								 int defaultTimeToLive) {
		this(aerospikeClient, aerospikeConverter, cacheNames, DEFAULT_SET_NAME, defaultTimeToLive);
	}

	/**
	 * Create a new {@link AerospikeCacheManager} instance -
	 * Specifying the set name and time to live configuration.
	 *
	 * @param aerospikeClient the instance that implements {@link IAerospikeClient}.
	 * @param aerospikeConverter the instance that implements {@link AerospikeConverter}.
	 * @param setName the set name.
	 * @param defaultTimeToLive the time to live configuration.
	 */
	public AerospikeCacheManager(IAerospikeClient aerospikeClient,
								 AerospikeConverter aerospikeConverter,
								 String setName,
								 int defaultTimeToLive) {
		this(aerospikeClient, aerospikeConverter, Collections.emptyList(), setName, defaultTimeToLive);
	}

	/**
	 * Create a new {@link AerospikeCacheManager} instance -
	 * Specifying the cache names (Aerospike namespaces), set name and the time to live configuration.
	 *
	 * @param aerospikeClient the instance that implements {@link IAerospikeClient}.
	 * @param aerospikeConverter the instance that implements {@link AerospikeConverter}.
	 * @param cacheNames the default caches to create.
	 * @param setName the set name.
	 * @param defaultTimeToLive the time to live configuration.
	 */
	public AerospikeCacheManager(IAerospikeClient aerospikeClient,
								 AerospikeConverter aerospikeConverter,
								 Collection<String> cacheNames,
								 String setName,
								 int defaultTimeToLive) {
		Assert.notNull(aerospikeClient, "The aerospike client must not be null");
		Assert.notNull(aerospikeConverter, "The aerospike converter must not be null");
		Assert.notNull(cacheNames, "Cache names must not be null");
		Assert.notNull(setName, "Set name must not be null");
		this.aerospikeClient = aerospikeClient;
		this.aerospikeConverter = aerospikeConverter;
		this.configuredCacheNames = new LinkedHashSet<>(cacheNames);
		this.setName = setName;
		this.defaultTimeToLive = defaultTimeToLive;
	}

	@Override
	protected Collection<? extends Cache> loadCaches() {
		List<AerospikeCache> caches = new ArrayList<>();
		for (String cacheName : configuredCacheNames) {
			caches.add(createCache(cacheName));
		}
		return caches;
	}

	@Override
	protected Cache getMissingCache(String cacheName) {
		return createCache(cacheName);
	}

	private AerospikeCache createCache(String cacheName) {
		return new AerospikeCache(aerospikeClient, aerospikeConverter, cacheName, setName, defaultTimeToLive);
	}

	protected Cache getAerospikeCache(String cacheName) {
		return getCache(cacheName);
	}

	protected Cache lookupAerospikeCache(String cacheName) {
		return lookupCache(AerospikeCacheUtils.getFullAerospikeCacheName(cacheName, setName));
	}

	@Override
	protected Cache decorateCache(Cache cache) {
		if (isCacheAlreadyDecorated(cache)) {
			return cache;
		}
		return super.decorateCache(cache);
	}

	private boolean isCacheAlreadyDecorated(Cache cache) {
		return isTransactionAware() && cache instanceof TransactionAwareCacheDecorator;
	}
}
