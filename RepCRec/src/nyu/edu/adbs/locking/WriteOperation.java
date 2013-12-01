package nyu.edu.adbs.locking;

public class WriteOperation {

  String variableName;
  int newValue;
  
  public WriteOperation(String variableName, int newValue) {
    this.variableName = variableName;
    this.newValue = newValue;
  }
  
  public String getVariableName() {
    return variableName;
  }
  
  public int getNewValue() {
    return newValue;
  }
  
}
