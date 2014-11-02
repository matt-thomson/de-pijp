package io.github.mattthomson.depijp.cascading;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class ListTupleEntryCollector<T> extends TupleEntryCollector {
    private final Fields field;
    private final List<T> values = new ArrayList<>();

    public ListTupleEntryCollector(Fields field) {
        super(field);

        this.field = field;
    }

    @Override
    protected void collect(TupleEntry tupleEntry) throws IOException {
        values.add((T) tupleEntry.getObject(field));
    }

    public List<T> getValues() {
        return ImmutableList.copyOf(values);
    }
}
