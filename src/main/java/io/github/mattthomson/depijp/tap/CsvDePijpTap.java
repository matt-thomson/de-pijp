package io.github.mattthomson.depijp.tap;

public class CsvDePijpTap extends DelimitedFileDePijpTap {
    public CsvDePijpTap(String path, int numFields) {
        this(path, numFields, false);
    }

    public CsvDePijpTap(String path, int numFields, boolean skipHeader) {
        super(path, numFields, ",", skipHeader);
    }
}