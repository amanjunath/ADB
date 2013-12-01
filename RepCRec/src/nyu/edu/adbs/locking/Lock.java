package nyu.edu.adbs.locking;

import nyu.edu.adbs.transaction.Transaction;

public class Lock {

  private Status status;
  private Type type;
  private Transaction transaction;
  private WriteOperation writeOperation;

  protected Lock(Transaction transaction, Type type, Status status) {
    this.type = type;
    this.status = status;
    this.transaction = transaction;
  }
  
  public enum Status {
    ACCEPT, DENY, BLOCK;
  }

  public enum Type {
    READ, WRITE;
  }

  public void setStatus(Status status) {
    this.status = status;
  }
  
  public Status getStatus() {
    return status;
  }

  public void setType(Type type) {
    this.type = type;
  }

  public Type getType() {
    return type;
  }
  
  public Transaction getTransaction() {
    return transaction;
  }
  
  public WriteOperation getWriteOperation() {
    return writeOperation;
  }
  
  public void setWriteOperation(WriteOperation writeOperation) {
    this.writeOperation = writeOperation;
  }

}
