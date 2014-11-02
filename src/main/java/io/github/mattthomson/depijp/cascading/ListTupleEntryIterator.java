package io.github.mattthomson.depijp.cascading;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

class ListTupleEntryIterator<T> extends TupleEntryIterator {
    private final Fields field;
    private final Iterator<T> delegate;

    public ListTupleEntryIterator(Fields field, List<T> values) {
        super(field);

        this.field = field;
        this.delegate = values.iterator();
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public TupleEntry next() {
        return new TupleEntry(field, new Tuple(delegate.next()));
    }
}
