package io.github.mattthomson.depijp.tap;

public class TsvDePijpTap extends DelimitedFileDePijpTap {
    public TsvDePijpTap(String path, int numFields) {
        super(path, numFields, "\t");
    }
}
