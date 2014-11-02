package io.github.mattthomson.depijp.sink;

import cascading.tap.Tap;
import cascading.tuple.Fields;

public interface PijpSink<T> {
    Tap createSinkTap(Fields field);
}
