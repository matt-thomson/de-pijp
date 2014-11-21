# De Pijp

[![Build Status](https://travis-ci.org/matt-thomson/de-pijp.svg?branch=develop)](https://travis-ci.org/matt-thomson/de-pijp)

De Pijp is a simple, type-safe, Apache-licensed Java 8 API for Hadoop MapReduce.  It's inspired by [Scalding](https://github.com/twitter/scalding), and provides an API that's similar to the streams API in Java 8.

## Example

The classic Hadoop word count example can be implemented in De Pijp like this:

```
public class WordCount implements DePijpFlow {
    @Override
    public void flow(PijpBuilder pijpBuilder, String[] args) {
        pijpBuilder
                .read(new TextLineDePijpTap(args[0]))

                .flatMap(line -> Arrays.stream(line.split("\\s+")))
                .group()
                .count()
                .map(KeyValue::toStringList)

                .write(new TsvDePijpTap(args[1], 2));
    }
}
```

To run this, package it into a JAR, and run:

```
hadoop jar <path-to-jar> io.github.mattthomson.DePijp <package>.WordCount <input> <output>
```

where `<input>` and `<output>` are paths in HDFS.

More examples can be found in the [De Pijp for the Impatient](https://github.com/matt-thomson/de-pijp-impatient) repo.

## De What?

De Pijp is named after a district in the South of Amsterdam.  It's pronounced roughly as "dee pipe".
