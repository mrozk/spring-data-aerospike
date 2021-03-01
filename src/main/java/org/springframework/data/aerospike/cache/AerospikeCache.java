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

import java.util.concurrent.Callable;

import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.aerospike.convert.AerospikeConverter;
import org.springframework.data.aerospike.convert.AerospikeReadData;
import org.springframework.data.aerospike.convert.AerospikeWriteData;

import java.util.Objects;

/**
 * 
 * @author Venil Noronha
 */
public class AerospikeCache implements Cache {

	private final Logger log = LoggerFactory.getLogger(AerospikeCache.class);

	private static final String VALUE = "value";

	protected IAerospikeClient client;
	protected AerospikeConverter aerospikeConverter;
	protected String namespace;
	protected String set;
	protected WritePolicy createOnly;
	protected WritePolicy writePolicyForPut;

	public AerospikeCache(IAerospikeClient client,
						  AerospikeConverter aerospikeConverter,
						  String namespace,
						  String set,
						  int expiration) {
		this.client = client;
		this.aerospikeConverter = aerospikeConverter;
		this.namespace = namespace;
		this.set = set;
		this.createOnly = new WritePolicy(client.getWritePolicyDefault());
		this.createOnly.recordExistsAction = RecordExistsAction.CREATE_ONLY;
		this.createOnly.expiration = expiration;
		this.writePolicyForPut = new WritePolicy(client.getWritePolicyDefault());
		this.writePolicyForPut.expiration = expiration;
	}

	protected Key getKey(Object key){
		return new Key(namespace, set, key.toString());
	}

	@Override
	public void clear() {
		client.truncate(null, namespace, set, null);
	}

	@Override
	public void evict(Object key) {
		client.delete(null, getKey(key));
	}

	@Override
	public String getName() {
		return this.namespace + ":" + this.set;
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
			} catch (Exception e) {
				log.warn("valueLoader exception", e);
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
