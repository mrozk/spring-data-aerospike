/**
 * 
 */
package org.springframework.data.aerospike.mapping;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.data.aerospike.sample.Person;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public class CachingAerospikePersistentPropertyTest {

	AerospikeMappingContext context;

	@BeforeEach
	public void setUp() {
		context = new AerospikeMappingContext();
		context.setApplicationContext(mock(ApplicationContext.class));
	}

	@Test
	public void testIsTransient() {
		AerospikePersistentEntity<?> entity = context.getPersistentEntity(Person.class);

		assertThat(entity.getIdProperty().isTransient()).isFalse();
	}

	@Test
	public void testIsAssociation() {
		AerospikePersistentEntity<?> entity = context.getPersistentEntity(Person.class);

		assertThat(entity.getIdProperty().isAssociation()).isFalse();
	}

	@Test
	public void testUsePropertyAccess() {
		AerospikePersistentEntity<?> entity = context.getPersistentEntity(Person.class);

		assertThat(entity.getIdProperty().usePropertyAccess()).isFalse();
	}

	@Test
	public void testIsIdProperty() {
		AerospikePersistentEntity<?> entity = context.getPersistentEntity(Person.class);

		assertThat(entity.getIdProperty().isIdProperty()).isTrue();
	}

	@Test
	public void testGetFieldName() {
		AerospikePersistentEntity<?> entity = context.getPersistentEntity(Person.class);

		assertThat(entity.getIdProperty().getName()).isEqualTo("id");
	}

}
