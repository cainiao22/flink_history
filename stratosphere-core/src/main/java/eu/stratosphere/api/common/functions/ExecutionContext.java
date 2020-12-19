package eu.stratosphere.api.common.functions;

public interface ExecutionContext {

    String getTaskName();

    int getNumberOfSubTasks();

    int getSubTaskIndex();
}
