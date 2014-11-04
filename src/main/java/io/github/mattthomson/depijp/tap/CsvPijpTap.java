package io.github.mattthomson.depijp.tap;

public class CsvPijpTap extends DelimitedFilePijpTap {
    public CsvPijpTap(String path, int numFields) {
        super(path, numFields, ",");
    }
}