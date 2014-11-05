package io.github.mattthomson.depijp;

import cascading.tap.Tap;
import cascading.tuple.TupleEntry;

public interface DePijpSource<T> {
    Tap createLocalSourceTap();

    Tap createHadoopSourceTap();

    T fromTupleEntry(TupleEntry tupleEntry);
}
