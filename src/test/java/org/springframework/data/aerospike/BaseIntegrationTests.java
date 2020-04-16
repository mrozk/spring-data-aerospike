package org.springframework.data.aerospike;

import com.playtika.test.aerospike.AerospikeTestOperations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.aerospike.config.TestConfig;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.concurrent.atomic.AtomicLong;

@ExtendWith(SpringExtension.class)
@SpringBootTest(
        classes = TestConfig.class,
        properties = {
                "expirationProperty: 1",
                "setSuffix: service1"
        }
)
public abstract class BaseIntegrationTests {

    private static final AtomicLong counter = new AtomicLong();

    @Value("${embedded.aerospike.namespace}")
    protected String namespace;

    protected String id;

    @Autowired
    protected AerospikeTestOperations aerospikeTestOperations;

    @Autowired
    protected BlockingAerospikeTestOperations blockingAerospikeTestOperations;

    @BeforeEach
    public void setUp() {
        this.id = nextId();
    }

    protected String getNameSpace() {
        return namespace;
    }

    protected static String nextId() {
        return "as-" + counter.incrementAndGet();
    }

}
