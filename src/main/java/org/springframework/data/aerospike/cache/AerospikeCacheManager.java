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
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.WritePolicy;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.support.SimpleValueWrapper;
import org.springframework.cache.transaction.AbstractTransactionSupportingCacheManager;
import org.springframework.cache.transaction.TransactionAwareCacheDecorator;
import org.springframework.data.aerospike.convert.AerospikeConverter;
import org.springframework.data.aerospike.convert.AerospikeReadData;
import org.springframework.data.aerospike.convert.AerospikeWriteData;
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
	 * Specifying the cache names.
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
	 * Specifying the cache names and set name.
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
	 * Specifying the cache names and time to live configuration.
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
	 * Specifying the cache names, set name and the time to live configuration.
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

	protected AerospikeCache createCache(String cacheName) {
		return new AerospikeSerializingCache(cacheName);
	}

	@Override
	public Cache getCache(String name) {
		Cache cache = lookupAerospikeCache(name);
		if (cache != null) {
			return cache;
		}
		else {
			Cache missingCache = getMissingCache(name);
			if (missingCache != null) {
				addCache(missingCache);
				return lookupAerospikeCache(name);  // may be decorated
			}
			return null;
		}
	}

	protected Cache lookupAerospikeCache(String name) {
		return lookupCache(name + ":" + setName);
	}

	@Override
	protected Cache decorateCache(Cache cache) {
		if (isCacheAlreadyDecorated(cache)) {
			return cache;
		}
		return super.decorateCache(cache);
	}

	protected boolean isCacheAlreadyDecorated(Cache cache) {
		return isTransactionAware() && cache instanceof TransactionAwareCacheDecorator;
	}

	public class AerospikeSerializingCache extends AerospikeCache {

		public AerospikeSerializingCache(String namespace) {
			super(aerospikeClient, namespace, setName, defaultTimeToLive);
		}

		@Override
		public <T> T get(Object key, Class<T> type) {
			Key dbKey = getKey(key);
			Record record =  client.get(null, dbKey);
			if (record != null) {
				AerospikeReadData data = AerospikeReadData.forRead(dbKey, record);
				T value = aerospikeConverter.read(type, data);
				return value;
			}
			return null;
		}

		@Override
		public ValueWrapper get(Object key) {
			Object value = get(key, Object.class);
			return (value != null ? new SimpleValueWrapper(value) : null);
		}

		@Override
		public void put(Object key, Object value) {
			serializeAndPut(writePolicyForPut, key, value);
		}

		@Override
		public ValueWrapper putIfAbsent(Object key, Object value) {
			serializeAndPut(createOnly, key, value);
			return get(key);
		}

		private void serializeAndPut(WritePolicy writePolicy, Object key, Object value) {
			AerospikeWriteData data = AerospikeWriteData.forWrite();
			aerospikeConverter.write(value, data);
			client.put(writePolicy, getKey(key), data.getBinsAsArray());
		}
	}
}
