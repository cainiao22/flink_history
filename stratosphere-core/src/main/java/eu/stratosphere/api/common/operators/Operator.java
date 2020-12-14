package eu.stratosphere.api.common.operators;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.utils.Visitable;

public abstract class Operator implements Visitable<Operator> {

    protected final Configuration parameters;

    protected CompilerHints compilerHints;

    protected String name;

    protected Operator(String name) {
        this.name = name;
        this.parameters = new Configuration();
        this.compilerHints = new CompilerHints();
    }
}
