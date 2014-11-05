package io.github.mattthomson.depijp;

import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public interface DePijpSink<T> {
    Tap createSinkTap();

    Tuple toTuple(T value);

    Fields getOutputFields();
}
