package io.github.mattthomson.depijp.tap;

public class TsvPijpTap extends DelimitedFilePijpTap {
    public TsvPijpTap(String path, int numFields) {
        super(path, numFields, "\t");
    }
}
