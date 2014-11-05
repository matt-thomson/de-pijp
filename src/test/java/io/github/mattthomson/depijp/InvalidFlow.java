package io.github.mattthomson.depijp;

import io.github.mattthomson.depijp.tap.TextLineDePijpTap;

public class InvalidFlow implements DePijpFlow {
    @Override
    public void flow(PijpBuilder pijpBuilder, String[] args) {
        pijpBuilder
                .read(new TextLineDePijpTap("src/test/resources/values.txt"));
    }
}
