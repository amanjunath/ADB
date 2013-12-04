package nyu.edu.adbs.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import nyu.edu.adbs.transaction.Transaction;

public class Site {

  private int siteId;
  private boolean isActive;
  private Map<String, Variable> variableMap;
  private Map<String, Variable> oldVariableMap;
  private Long lastFailedTime;
  
  public Long getLastFailedTime() {
    return lastFailedTime;
  }

  public String getVariables() {
    if(!isActive) {
      return "FAIL";
    }
    StringBuilder variablesBuilder = new StringBuilder();
    TreeMap<Integer, Variable> sortedVariables = 
        new TreeMap<Integer, Variable>();
    
    for( Variable variable : variableMap.values() ) {
      sortedVariables.put(variable.getVariableId(), variable);
    }
    
    for( Map.Entry<Integer, Variable> variableEntry 
        : sortedVariables.entrySet() ) {
      variablesBuilder.append(variableEntry.getValue().getVariableName())
          .append(" = ").append(variableEntry.getValue()
              .getValue()).append(", ");
    }
    variablesBuilder.replace(variablesBuilder.lastIndexOf(", ")
        , variablesBuilder.length(), "");
    return variablesBuilder.toString();
  }
  
  public Site(int siteId, Map<String,Variable> variableMap, 
      Map<String, Transaction> writeLockTable, 
          Map<String, Set<Transaction>> readLockTable) {
    
    if( variableMap == null ) {
      throw new NullPointerException("variableMap cannot be null");
    }
    this.siteId = siteId;
    this.isActive = true;
    this.variableMap = new HashMap<String, Variable>(variableMap);
    this.oldVariableMap = new HashMap<String, Variable>();
    for(Variable variable : variableMap.values() ) {
      oldVariableMap.put(variable.getVariableName(), new Variable(variable));
    }
  }
  
  public int getSiteId() {
    return siteId;
  }
  
  public boolean isActive() {
    return isActive;
  }
  
  public void fail(Long timestamp) {
    isActive = false;
    lastFailedTime = timestamp;
  }
  
  public void recover() {
    isActive = true;
  }
  
  /**
   * 
   * @param variableName
   * @return
   * @throws NullPointerException if value is not present here. Use with
   * function variableExists(String variableName)
   */
  public int readOld(String variableName) {
    return oldVariableMap.get(variableName).getValue();
  }
  
  /**
   * 
   * @param variableName
   * @return
   * @throws NullPointerException if value is not present here. Use with
   * function variableExists(String variableName)
   */
  public int read(String variableName) {
    return variableMap.get(variableName).getValue();
  }
  
  public boolean variableExists(String variableName) {
    return variableMap.containsKey(variableName);
  }
  
  public void write(String variableName, int newValue) {
    Variable variable = variableMap.get(variableName);
    variable.setValue(newValue);
    //not sure if we should make a new variable
  }
  
  public void rollback(String variableName) {
    Variable variable = variableMap.get(variableName);
    variable.setValue(oldVariableMap.get(variableName).getValue()); 
  }
  
  public void commit(String variableName) {
    Variable variable = oldVariableMap.get(variableName);
    variable.setValue(variableMap.get(variableName).getValue()); 
  }
  
  @Override
  public String toString() {
    return "Site [siteId=" + siteId + ", isActive=" + isActive
        + ", variableMap=" + variableMap + "]";
  }

  
}
