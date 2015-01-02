package io.github.mattthomson.depijp;

import java.io.Serializable;

public interface DePijpFlow extends Serializable {
    void flow(PijpBuilder pijpBuilder, String[] args);
}
