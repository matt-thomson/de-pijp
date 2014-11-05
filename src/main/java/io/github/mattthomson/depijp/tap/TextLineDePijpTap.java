package io.github.mattthomson.depijp.tap;

import cascading.scheme.Scheme;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

public class TextLineDePijpTap extends FileDePijpTap<String> {
    private static final Fields FIELD = new Fields("line");

    public TextLineDePijpTap(String path) {
        super(path);
    }

    @Override
    protected Scheme<Properties, InputStream, OutputStream, ?, ?> getLocalScheme() {
        return new cascading.scheme.local.TextLine(FIELD);
    }

    @Override
    protected Scheme<JobConf, RecordReader, OutputCollector, ?, ?> getHadoopScheme() {
        return new cascading.scheme.hadoop.TextLine(FIELD);
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
