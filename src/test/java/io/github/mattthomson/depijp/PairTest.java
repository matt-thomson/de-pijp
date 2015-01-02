package io.github.mattthomson.depijp;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PairTest {
    @Test
    public void shouldObeyEqualsHashCodeContract() {
        EqualsVerifier.forClass(Pair.class).verify();
    }

    @Test
    public void shouldConvertPairToStringList() {
        Pair<Integer, Boolean> pair = new Pair<>(123, true);
        assertThat(pair.toStringList()).containsExactly("123", "true");
    }

    @Test
    public void shouldMapFirst() {
        Pair<Integer, Boolean> pair = new Pair<>(123, true);
        Pair<String, Boolean> mapped = pair.mapFirst(x -> Integer.toString(x));
        assertThat(mapped).isEqualTo(new Pair<>("123", true));
    }

    @Test
    public void shouldMapSecond() {
        Pair<Integer, Boolean> pair = new Pair<>(123, true);
        Pair<Integer, String> mapped = pair.mapSecond(x -> Boolean.toString(x));
        assertThat(mapped).isEqualTo(new Pair<>(123, "true"));
    }
}