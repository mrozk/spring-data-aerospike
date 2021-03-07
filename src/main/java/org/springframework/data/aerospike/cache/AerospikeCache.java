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
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;
import org.springframework.data.aerospike.convert.AerospikeConverter;
import org.springframework.data.aerospike.convert.AerospikeReadData;
import org.springframework.data.aerospike.convert.AerospikeWriteData;

import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * A Cache {@link org.springframework.cache.Cache} implementation backed by Aerospike database as store.
 * Create and configure Aerospike cache instances via {@link AerospikeCacheManager}.
 *
 * @author Venil Noronha
 */
public class AerospikeCache implements Cache {

	private static final String VALUE = "value";

	private final String name;
	private final IAerospikeClient client;
	private final AerospikeConverter aerospikeConverter;
	private final AerospikeCacheConfiguration cacheConfiguration;
	private final WritePolicy createOnly;
	private final WritePolicy writePolicyForPut;

	public AerospikeCache(String name,
						  IAerospikeClient client,
						  AerospikeConverter aerospikeConverter,
						  AerospikeCacheConfiguration cacheConfiguration) {
		this.name = name;
		this.client = client;
		this.aerospikeConverter = aerospikeConverter;
		this.cacheConfiguration = cacheConfiguration;
		this.createOnly = new WritePolicy(client.getWritePolicyDefault());
		this.createOnly.recordExistsAction = RecordExistsAction.CREATE_ONLY;
		this.createOnly.expiration = cacheConfiguration.getExpirationInSeconds();
		this.writePolicyForPut = new WritePolicy(client.getWritePolicyDefault());
		this.writePolicyForPut.expiration = cacheConfiguration.getExpirationInSeconds();
	}

	private Key getKey(Object key){
		return new Key(cacheConfiguration.getNamespace(), cacheConfiguration.getSet(), key.toString());
	}

	@Override
	public void clear() {
		client.truncate(null, cacheConfiguration.getNamespace(), cacheConfiguration.getSet(), null);
	}

	@Override
	public void evict(Object key) {
		client.delete(null, getKey(key));
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public Object getNativeCache() {
		return client;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T get(Object key, Callable<T> valueLoader) {
		T value = (T) client.get(null, getKey(key)).getValue(VALUE);
		if (Objects.isNull(value)) {
			try {
				value = valueLoader.call();
				if (Objects.nonNull(value)) {
					put(key, value);
				}
			} catch (Throwable e) {
				throw new Cache.ValueRetrievalException(key, valueLoader, e);
			}
		}
		return value;
	}

	@Override
	public <T> T get(Object key, Class<T> type) {
		Key dbKey = getKey(key);
		Record record =  client.get(null, dbKey);
		if (record != null) {
			AerospikeReadData data = AerospikeReadData.forRead(dbKey, record);
			return aerospikeConverter.read(type, data);
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
