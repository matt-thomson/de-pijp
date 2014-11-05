package io.github.mattthomson.depijp;

import io.github.mattthomson.depijp.tap.TextLinePijpTap;

public class InvalidFlow implements PijpFlow {
    @Override
    public void flow(PijpBuilder pijpBuilder, String[] args) {
        pijpBuilder
                .read(new TextLinePijpTap("src/test/resources/values.txt"));
    }
}
