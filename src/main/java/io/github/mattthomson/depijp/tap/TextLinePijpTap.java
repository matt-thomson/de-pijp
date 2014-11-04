package io.github.mattthomson.depijp.tap;

import cascading.scheme.Scheme;
import cascading.scheme.local.TextLine;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

public class TextLinePijpTap extends FilePijpTap<String> {
    private static final Fields FIELD = new Fields("line");

    public TextLinePijpTap(String path) {
        super(path);
    }

    @Override
    protected Scheme<Properties, InputStream, OutputStream, ?, ?> getScheme() {
        return new TextLine(FIELD);
    }

    @Override
    public String fromTupleEntry(TupleEntry tupleEntry) {
        return tupleEntry.getString(FIELD);
    }

    @Override
    public Tuple toTuple(String value) {
        return new Tuple(value);
    }

    @Override
    public Fields getOutputFields() {
        return FIELD;
    }
}
