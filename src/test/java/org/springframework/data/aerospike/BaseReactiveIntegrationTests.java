package org.springframework.data.aerospike;

import com.aerospike.client.lua.LuaAerospikeLib;
import com.aerospike.client.reactor.AerospikeReactorClient;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.Mono;

import java.io.Serializable;
import java.time.Duration;

public abstract class BaseReactiveIntegrationTests extends BaseIntegrationTests {

    @Autowired
    protected ReactiveAerospikeTemplate reactiveTemplate;
    @Autowired
    protected AerospikeReactorClient reactorClient;

    protected <T> T findById(Serializable id, Class<T> type) {
        return reactiveTemplate.findById(id, type).block();
    }

    @BeforeClass
    public static void installBlockHound() {
        BlockHound.install();
    }

    @Test(expected = RuntimeException.class)
    public void shouldFailAsBlocking(){
        Mono.delay(Duration.ofSeconds(1))
                .doOnNext(it -> {
                    try {
                        Thread.sleep(10);
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                })
                .block();
    }

}