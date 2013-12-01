package nyu.edu.adbs.transaction;

import java.util.Map;

public class ReadOnlyTransaction extends Transaction {

  public Map<String, Integer> variablesCopyMap;
  
  public ReadOnlyTransaction(String name, long timestamp, Type type, 
      State state, Map<String, Integer> variableCopyMap ) {
    super(name, timestamp, type, state);
    this.variablesCopyMap = variableCopyMap;
  }

  /**
   * @throws NullPointerException if variableId is invalid
   */
  @Override
  public Integer read(String variableName, long timestamp) {
    return variablesCopyMap.get(variableName);
  }

  @Override
  public boolean write(String variableName, int value, long timestamp) {
    throw new UnsupportedOperationException("This is a readonly transaction");
  }

  @Override
  public void abort() {
    
  }
  
}