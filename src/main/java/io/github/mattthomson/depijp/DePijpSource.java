package io.github.mattthomson.depijp;

import cascading.tap.Tap;
import cascading.tuple.TupleEntry;

public interface DePijpSource<T> {
    Tap createSourceTap();

    T fromTupleEntry(TupleEntry tupleEntry);
}
