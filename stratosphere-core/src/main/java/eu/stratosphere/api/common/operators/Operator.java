package eu.stratosphere.api.common.operators;

import eu.stratosphere.api.common.operators.util.UserCodeWrapper;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.utils.Visitable;
import java.lang.annotation.Annotation;
import java.util.List;

public abstract class Operator implements Visitable<Operator> {

    protected final Configuration parameters;

    protected CompilerHints compilerHints;

    protected String name;

    protected List<? extends Annotation> ocs;

    private int degreeOfParallelism = -1;

    protected Operator(String name) {
        this.name = name;
        this.parameters = new Configuration();
        this.compilerHints = new CompilerHints();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Configuration getParameters() {
        return parameters;
    }

    public void setParameter(String key, String value){
       parameters.setString(key, value);
    }

    public void setParameter(String key, Integer value){
        parameters.setInteger(key, value);
    }

    public void setParameter(String key, Boolean value){
        parameters.setBoolean(key, value);
    }

    public CompilerHints getCompilerHints() {
        return compilerHints;
    }

    public void setCompilerHints(CompilerHints compilerHints) {
        this.compilerHints = compilerHints;
    }

    public int getDegreeOfParallelism() {
        return degreeOfParallelism;
    }

    public void setDegreeOfParallelism(int degreeOfParallelism) {
        this.degreeOfParallelism = degreeOfParallelism;
    }

    public UserCodeWrapper<?> getUserCodeWrapper(){
        return null;
    }

    public <A extends Annotation> A getUserCodeAnnotation(Class<A> annotationClass){
        return getUserCodeWrapper().getUserCodeAnnotation(annotationClass);
    }
}
