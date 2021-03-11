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
import lombok.Builder;
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
@Builder
public class AerospikeCacheManager extends AbstractTransactionSupportingCacheManager {

	private final IAerospikeClient aerospikeClient;
	private final AerospikeConverter aerospikeConverter;
	private final AerospikeCacheConfiguration defaultCacheConfiguration;
	private final Map<String, AerospikeCacheConfiguration> initialCacheConfiguration;

	/**
	 * Create a new {@link AerospikeCacheManager} instance -
	 * Without specifying any additional parameter.
	 *
	 * @param aerospikeClient the instance that implements {@link IAerospikeClient}.
	 * @param aerospikeConverter the instance that implements {@link AerospikeConverter}.
	 */
	public AerospikeCacheManager(IAerospikeClient aerospikeClient,
								 AerospikeConverter aerospikeConverter) {
		this(aerospikeClient, aerospikeConverter, new AerospikeCacheConfiguration(), new LinkedHashMap<>());
	}

	/**
	 * Create a new {@link AerospikeCacheManager} instance -
	 * Specifying one or more initial caches.
	 * Caches will be configured with default configuration.
	 *
	 * @param aerospikeClient the instance that implements {@link IAerospikeClient}.
	 * @param aerospikeConverter the instance that implements {@link AerospikeConverter}.
	 * @param initialCacheNames the names of the caches to initialize.
	 */
	public AerospikeCacheManager(IAerospikeClient aerospikeClient,
								 AerospikeConverter aerospikeConverter,
								 String... initialCacheNames) {
		this(aerospikeClient, aerospikeConverter);
		AerospikeCacheConfiguration defaultCacheConfiguration = new AerospikeCacheConfiguration();

		for (String cacheName : initialCacheNames) {
			initialCacheConfiguration.put(cacheName, defaultCacheConfiguration);
		}
	}

	/**
	 * Create a new {@link AerospikeCacheManager} instance -
	 * Specifying a default cache configuration and one or more initial caches.
	 * Caches will be configured with provided default cache configuration.
	 *
	 * @param aerospikeClient the instance that implements {@link IAerospikeClient}.
	 * @param aerospikeConverter the instance that implements {@link AerospikeConverter}.
	 * @param defaultCacheConfiguration the default cache configuration.
	 * @param initialCacheNames the names of the caches to initialize.
	 */
	public AerospikeCacheManager(IAerospikeClient aerospikeClient,
								 AerospikeConverter aerospikeConverter,
								 AerospikeCacheConfiguration defaultCacheConfiguration,
								 String... initialCacheNames) {
		this(aerospikeClient, aerospikeConverter);

		for (String cacheName : initialCacheNames) {
			initialCacheConfiguration.put(cacheName, defaultCacheConfiguration);
		}
	}

	/**
	 * Create a new {@link AerospikeCacheManager} instance -
	 * Specifying the default cache configuration and a map of caches and matching configurations.
	 *
	 * @param aerospikeClient the instance that implements {@link IAerospikeClient}.
	 * @param aerospikeConverter the instance that implements {@link AerospikeConverter}.
	 * @param defaultCacheConfiguration the default aerospike cache configuration.
	 * @param initialCacheConfiguration a map of caches and matching configurations.
	 */
	public AerospikeCacheManager(IAerospikeClient aerospikeClient,
								 AerospikeConverter aerospikeConverter,
								 AerospikeCacheConfiguration defaultCacheConfiguration,
								 Map<String, AerospikeCacheConfiguration> initialCacheConfiguration) {
		Assert.notNull(aerospikeClient, "The aerospike client must not be null");
		Assert.notNull(aerospikeConverter, "The aerospike converter must not be null");
		this.aerospikeClient = aerospikeClient;
		this.aerospikeConverter = aerospikeConverter;
		this.defaultCacheConfiguration = (defaultCacheConfiguration != null ? defaultCacheConfiguration : new AerospikeCacheConfiguration());
		this.initialCacheConfiguration = (initialCacheConfiguration != null ? initialCacheConfiguration : new LinkedHashMap<>());
	}

	@Override
	protected Collection<? extends Cache> loadCaches() {
		List<AerospikeCache> caches = new ArrayList<>();
		for (Map.Entry<String, AerospikeCacheConfiguration> entry : initialCacheConfiguration.entrySet()) {
			caches.add(createCache(entry.getKey(), entry.getValue()));
		}
		return caches;
	}

	@Override
	protected Cache getMissingCache(String name) {
		return createCache(name);
	}

	@Override
	protected Cache decorateCache(Cache cache) {
		if (isCacheAlreadyDecorated(cache)) {
			return cache;
		}
		return super.decorateCache(cache);
	}

	private AerospikeCache createCache(String name) {
		return new AerospikeCache(name, aerospikeClient, aerospikeConverter, defaultCacheConfiguration);
	}

	private AerospikeCache createCache(String name, AerospikeCacheConfiguration cacheConfiguration) {
		return new AerospikeCache(name, aerospikeClient, aerospikeConverter, cacheConfiguration);
	}

	private boolean isCacheAlreadyDecorated(Cache cache) {
		return isTransactionAware() && cache instanceof TransactionAwareCacheDecorator;
	}
}
