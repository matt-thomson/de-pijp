package io.github.mattthomson.depijp.cascading;

import cascading.operation.ConcreteCall;
import cascading.operation.Filter;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FilterOperationTest {
    @Test
    public void shouldRemoveWhenPredicateIsFalse() {
        Fields field = new Fields("field");
        Filter<Void> filter = new FilterOperation<>(x -> false, field);

        TupleEntry argument = new TupleEntry(field, new Tuple(1));
        ListTupleEntryCollector<Integer> collector = new ListTupleEntryCollector<>(field);

        assertThat(filter.isRemove(null, new ConcreteCall<>(argument, collector))).isTrue();
    }

    @Test
    public void shouldNotRemoveWhenPredicateIsTrue() {
        Fields field = new Fields("field");
        Filter<Void> filter = new FilterOperation<>(x -> true, field);

        TupleEntry argument = new TupleEntry(field, new Tuple(1));
        ListTupleEntryCollector<Integer> collector = new ListTupleEntryCollector<>(field);

        assertThat(filter.isRemove(null, new ConcreteCall<>(argument, collector))).isFalse();
    }
}