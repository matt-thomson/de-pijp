package io.github.mattthomson.depijp;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.List;
import java.util.function.Function;

public class Pair<S, T> implements Serializable {
    private final S first;
    private final T second;

    public Pair(S first, T second) {
        this.first = first;
        this.second = second;
    }

    public S getFirst() {
        return first;
    }

    public T getSecond() {
        return second;
    }

    public <U> Pair<U, T> mapFirst(Function<S, U> fn) {
        return new Pair<>(fn.apply(first), second);
    }

    public <U> Pair<S, U> mapSecond(Function<T, U> fn) {
        return new Pair<>(first, fn.apply(second));
    }

    public List<String> toStringList() {
        return ImmutableList.of(first.toString(), second.toString());
    }

    @Override
    public final int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public final boolean equals(Object o) {
        return EqualsBuilder.reflectionEquals(this, o);
    }

    @Override
    public final String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
