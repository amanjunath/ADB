package nyu.edu.adbs.storage;

public class Variable {
  
  private int variableId;
  private String variableName;
  private int value;
  
  public Variable(int variableId, String variableName, int value) {
    if(variableName == null) {
      throw new NullPointerException("variableName cannot be null");
    }
    this.variableId = variableId;
    this.variableName = variableName;
    this.value = value;
  }
  
  public Variable(Variable variable) {
    if(variable == null) {
      throw new NullPointerException("variable cannot be null");
    }
    this.variableId = variable.getVariableId();
    this.variableName = variable.getVariableName();
    this.value = variable.getValue();
  }

  public int getVariableId() {
    return variableId;
  }

  public String getVariableName() {
    return variableName;
  }

  public int getValue() {
    return value;
  }

  public void setValue(int value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "Variable [variableId=" + variableId + ", variableName="
        + variableName + ", value=" + value + "]";
  }
  
}