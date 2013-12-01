package nyu.edu.adbs.transaction;

abstract public class Transaction {

  private Type type;
  private State state;
  private String name;
  private long timestamp;

  public enum Type {
    READ_ONlY, READ_WRITE;
  }

  public enum State {
    ACTIVE, COMMITED, BLOCKED, ABORTED;
  }

  public Transaction(String name, long timestamp, Type type, State state) {
    this.name = name;
    this.timestamp = timestamp;
    this.type = type;
    this.state = state;
  }

  public Type getType() {
    return type;
  }

  public State getState() {
    return state;
  }

  public String getName() {
    return name;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void begin() {
    
  }

  public boolean commit() {
    return true;
  }

  public void setType(Type type) {
    this.type = type;
  }

  public void setState(State state) {
    this.state = state;
  }
  
  abstract public void abort();
  
  abstract public Integer read(String variableName, long timestamp);

  abstract public boolean write(String variableId, int value, long timestamp);
  
}
