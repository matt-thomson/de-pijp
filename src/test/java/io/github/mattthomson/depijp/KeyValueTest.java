package io.github.mattthomson.depijp;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class KeyValueTest {
    @Test
    public void shouldObeyEqualsHashCodeContract() {
        EqualsVerifier.forClass(KeyValue.class).verify();
    }

    @Test
    public void shouldConvertKeyValueToStringList() {
        KeyValue<Integer, Boolean> kv = new KeyValue<>(123, true);
        assertThat(kv.toStringList()).containsExactly("123", "true");
    }
}