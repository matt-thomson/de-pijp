package io.github.mattthomson.depijp.tap;

import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.Tuples;
import com.google.common.collect.ImmutableList;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TsvPijpTap extends FilePijpTap<List<String>> {
    private final List<Fields> fields;

    public TsvPijpTap(String path, int numFields) {
        super(path);

        fields = IntStream.range(0, numFields)
                .mapToObj(i -> new Fields(String.format("tsv-field-%s", i)))
                .collect(Collectors.toList());
    }

    @Override
    protected Scheme<Properties, InputStream, OutputStream, ?, ?> getScheme() {
        return new TextDelimited(getOutputFields(), "\t");
    }

    @Override
    public Fields getOutputFields() {
        return fields.stream()
                .collect(Collectors.reducing(new Fields(), Fields::append));
    }

    @Override
    public List<String> fromTupleEntry(TupleEntry tupleEntry) {
        return fields.stream()
                .map(tupleEntry::getString)
                .collect(Collectors.toList());
    }

    @Override
    public Tuple toTuple(List<String> value) {
        return Tuples.create(ImmutableList.copyOf(value));
    }
}
