package org.springframework.data.aerospike.mapping;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.aerospike.SampleClasses.DocumentWithExpressionInCollection;
import org.springframework.data.aerospike.SampleClasses.DocumentWithoutCollection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
public class BasicAerospikePersistentEntityTest {

    private AerospikeMappingContext context = new AerospikeMappingContext();

    @Test
    public void shouldReturnSimpleClassNameIfCollectionNotSpecified() {
        BasicAerospikePersistentEntity<?> entity = context.getPersistentEntity(DocumentWithoutCollection.class);

        assertThat(entity.getSetName()).isEqualTo(DocumentWithoutCollection.class.getSimpleName());
    }

    @Test
    public void shouldFailIfEnvironmentNull() {
        BasicAerospikePersistentEntity<?> entity = context.getPersistentEntity(DocumentWithExpressionInCollection.class);

        assertThatThrownBy(() -> entity.getSetName())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Environment must be set to use 'collection'");
    }

}
