package eu.stratosphere.api.common.operators;

import eu.stratosphere.api.common.functions.Function;
import eu.stratosphere.api.common.operators.util.UserCodeWrapper;
import eu.stratosphere.utils.Visitor;
import java.util.ArrayList;
import java.util.List;

/**
 * 单一输入源的操作基础类
 *
 * @author yanpengfei
 * @date 2020/12/24
 **/
public class SingleInputOperator<T extends Function> extends AbstractUdfOperator<T> {

    protected final List<Operator> input = new ArrayList<>();

    private final int[] keyFields;

    public SingleInputOperator(UserCodeWrapper<T> stub, int[] keyPositions, String name) {
        super(stub, name);
        this.keyFields = keyPositions;
    }

    protected SingleInputOperator(UserCodeWrapper<T> stub, String name) {
        super(stub, name);
        this.keyFields = new int[0];
    }

    public List<Operator> getInputs() {
        return input;
    }

    public void clearInputs() {
        this.input.clear();
    }

    public void addInput(Operator... input) {
        for (Operator operator : input) {
            if (operator == null) {
                throw new IllegalArgumentException("The input may not contain null elements.");
            } else {
                this.input.add(operator);
            }
        }
    }

    public void addInputs(List<Operator> inputs) {
        for (Operator input : inputs) {
            this.addInput(input);
        }
    }

    public void setInput(Operator... input) {
        this.input.clear();
        addInput(input);
    }

    public void setInputs(List<Operator> inputs) {
        this.input.clear();
        addInputs(inputs);
    }

    @Override
    public int getNumberOfInputs() {
        return 1;
    }

    @Override
    public int[] getKeyColumns(int inputNum) {
        if (inputNum == 0) {
            return this.keyFields;
        }
        throw new IndexOutOfBoundsException();
    }

    @Override
    public void accept(Visitor<Operator> visitor) {
        if (visitor.preVisit(this)) {
            for (Operator operator : this.input) {
                operator.accept(visitor);
            }
            visitor.postVisit(this);
        }
    }
}
