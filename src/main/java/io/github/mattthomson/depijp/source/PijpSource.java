package io.github.mattthomson.depijp.source;

import cascading.tap.Tap;
import cascading.tuple.Fields;

public interface PijpSource<T> {
    Tap createSourceTap(Fields field);
}
