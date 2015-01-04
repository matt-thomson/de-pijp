package io.github.mattthomson.depijp.tap;

public class TsvDePijpTap extends DelimitedFileDePijpTap {
    public TsvDePijpTap(String path, int numFields) {
        this(path, numFields, false);
    }

    public TsvDePijpTap(String path, int numFields, boolean skipHeader) {
        super(path, numFields, "\t", skipHeader);
    }
}
