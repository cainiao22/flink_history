package eu.stratosphere.api.common.operators;

import eu.stratosphere.api.common.functions.Function;
import eu.stratosphere.api.common.operators.util.UserCodeWrapper;
import eu.stratosphere.utils.Visitor;
import java.util.ArrayList;
import java.util.List;

/**
 * 拥有两个输入变量的聚合操作的统一父类
 *
 * @author yanpengfei
 * @date 2020/12/23
 **/
public abstract class DualInputOperator<T extends Function> extends AbstractUdfOperator<T> {

    protected final List<Operator> input1 = new ArrayList<>();

    protected final List<Operator> input2 = new ArrayList<>();

    private final int[] keyFields1;

    private final int[] keyFields2;

    public DualInputOperator() {
        this(null, null);
    }

    protected DualInputOperator(UserCodeWrapper<T> stub,
        String name) {
        super(stub, name);
        this.keyFields1 = this.keyFields2 = new int[0];
    }

    protected DualInputOperator(UserCodeWrapper<T> stub, int[] keyPositions1, int[] keyPositions2,
        String name) {
        super(stub, name);
        this.keyFields1 = keyPositions1;
        this.keyFields2 = keyPositions2;
    }

    public List<Operator> getFirstInputs() {
        return this.input1;
    }

    public List<Operator> getSecondInputs() {
        return this.input2;
    }

    public void clearFirstInputs() {
        this.input1.clear();
    }

    public void clearSecondInputs() {
        this.input2.clear();
    }

    public void addFirstInput(Operator... input) {
        for (Operator operator : input) {
            if (operator == null) {
                throw new IllegalArgumentException("The input may not contain null elements.");
            }
            this.input1.add(operator);
        }
    }

    public void addSecondInput(Operator... input) {
        for (Operator operator : input) {
            if (operator == null) {
                throw new IllegalArgumentException("The input may not contain null elements.");
            }
            this.input2.add(operator);
        }
    }

    public void addFirstInputs(List<Operator> input) {
        for (Operator operator : input) {
            if (operator == null) {
                throw new IllegalArgumentException("The input may not contain null elements.");
            }
            this.input1.add(operator);
        }
    }

    public void addSecondInputs(List<Operator> input) {
        for (Operator operator : input) {
            if (operator == null) {
                throw new IllegalArgumentException("The input may not contain null elements.");
            }
            this.input2.add(operator);
        }
    }

    public void setFirstInput(Operator input) {
        this.input1.clear();
        addFirstInput(input);
    }

    public void setSecondInput(Operator input) {
        this.input2.clear();
        addSecondInput(input);
    }

    public void setFirstInputs(List<Operator> inputs) {
        this.input1.clear();
        addFirstInputs(inputs);
    }

    public void setSecondInputs(List<Operator> inputs) {
        this.input2.clear();
        addSecondInputs(inputs);
    }

    @Override
    public int getNumberOfInputs() {
        return 2;
    }

    @Override
    public int[] getKeyColumns(int inputNum) {
        if (inputNum == 0) {
            return keyFields1;
        }
        if (inputNum == 1) {
            return keyFields2;
        }
        throw new IndexOutOfBoundsException();
    }

    @Override
    public void accept(Visitor<Operator> visitor) {
        boolean descend = visitor.preVisit(this);
        if (descend) {
            for (Operator c : input1) {
                c.accept(visitor);
            }
            for (Operator c : input2) {
                c.accept(visitor);
            }
            visitor.postVisit(this);
        }
    }
}
