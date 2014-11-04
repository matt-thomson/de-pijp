package io.github.mattthomson.depijp;

import cascading.tap.Tap;
import cascading.tuple.TupleEntry;

public interface PijpSource<T> {
    Tap createSourceTap();

    T fromTupleEntry(TupleEntry tupleEntry);
}
