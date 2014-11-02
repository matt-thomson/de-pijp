package io.github.mattthomson.depijp.tap;

import cascading.scheme.Scheme;
import cascading.scheme.local.TextLine;
import cascading.tuple.Fields;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

public class TextLinePijpTap extends FilePijpTap<String> {
    public TextLinePijpTap(String path) {
        super(path);
    }

    @Override
    protected Scheme<Properties, InputStream, OutputStream, ?, ?> getScheme(Fields field) {
        return new TextLine(field);
    }
}
