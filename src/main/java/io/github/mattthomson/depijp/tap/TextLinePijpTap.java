package io.github.mattthomson.depijp.tap;

import cascading.scheme.local.TextLine;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import io.github.mattthomson.depijp.PijpTap;

import static cascading.tap.SinkMode.REPLACE;

public class TextLinePijpTap implements PijpTap<String> {
    private final String path;

    public TextLinePijpTap(String path) {
        this.path = path;
    }

    @Override
    public Tap createTap(Fields field) {
        return new FileTap(new TextLine(field), path, REPLACE);
    }
}
