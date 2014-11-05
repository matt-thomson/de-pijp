package io.github.mattthomson.depijp.tap;

public class CsvDePijpTap extends DelimitedFileDePijpTap {
    public CsvDePijpTap(String path, int numFields) {
        super(path, numFields, ",");
    }
}