package nyu.edu.adbs.locking;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import nyu.edu.adbs.transaction.Transaction;

public class LockManager {

  private Map<String, Transaction> writeLocks;
  private Map<String, Set<Transaction>> readLocks;
  private Map<Transaction, Set<String>> transactionVariableWriteLocks;
  private Map<Transaction, Set<String>> transactionVariableReadLocks;
  
  public LockManager(Map<String, Transaction> writeLocks, 
      Map<String, Set<Transaction>> readLocks) {
    this.writeLocks = new HashMap<String, Transaction>();
    this.readLocks = new HashMap<String, Set<Transaction>>();
    this.transactionVariableWriteLocks 
        = new HashMap<Transaction, Set<String>>();
    this.transactionVariableReadLocks
        = new HashMap<Transaction, Set<String>>();
  }
  
  public Lock acquireRead(Transaction transaction, String variableName) {
    if( writeLocks.containsKey(variableName) ) {
      Transaction lockingTransaction = writeLocks.get(variableName);
      if( lockingTransaction.equals(transaction) ) {
        return new Lock(transaction, Lock.Type.READ, Lock.Status.ACCEPT);
      }
      else if( transaction.getTimestamp() < 
          lockingTransaction.getTimestamp() ) {
        return new Lock(transaction, Lock.Type.READ, Lock.Status.BLOCK);
      }
      return new Lock(transaction, Lock.Type.READ, Lock.Status.DENY);
    }
    Set<Transaction> readTransactions = readLocks.get(variableName);
    if( readTransactions == null ) {
      readTransactions = new HashSet<Transaction>();
      readLocks.put(variableName, readTransactions);
    }
    readTransactions.add(transaction);
    
    Set<String> readVariables = transactionVariableReadLocks.get(transaction);
    if( readVariables == null ) {
      readVariables = new HashSet<String>();
      transactionVariableReadLocks.put(transaction, readVariables);
    }
    readVariables.add(variableName);
    return new Lock(transaction, Lock.Type.READ, Lock.Status.ACCEPT);
  }
  
  public Lock acquireWrite(Transaction transaction, String variableName) {
    if( writeLocks.containsKey(variableName) ) {
      Transaction lockingTransaction = writeLocks.get(variableName);
      if( lockingTransaction.equals(transaction) ) {
        return new Lock(transaction, Lock.Type.WRITE, Lock.Status.ACCEPT);
      }
      else if( transaction.getTimestamp() < 
          lockingTransaction.getTimestamp() ) {
        return new Lock(transaction, Lock.Type.WRITE, Lock.Status.BLOCK);
      }
      return new Lock(transaction, Lock.Type.WRITE, Lock.Status.DENY);
    }
    Set<Transaction> transactionsOnReadLock = readLocks.get(variableName);
    if( transactionsOnReadLock == null || 
        (transactionsOnReadLock.contains(transaction) &&
            transactionsOnReadLock.size() == 1) ) {
      writeLocks.put(variableName, transaction);
      
      Set<String> writeVariables = 
          transactionVariableWriteLocks.get(transaction);
      if( writeVariables == null ) {
        writeVariables = new HashSet<String>();
        transactionVariableWriteLocks.put(transaction, writeVariables);
      }
      writeVariables.add(variableName);
      
      return new Lock(transaction, Lock.Type.WRITE, Lock.Status.ACCEPT);
    }
    for( Transaction transactionOnReadLock : transactionsOnReadLock ) {
      if( transactionOnReadLock.getTimestamp() <= transaction.getTimestamp() ) {
        return new Lock(transaction, Lock.Type.WRITE, Lock.Status.DENY);
      }
    }
    return new Lock(transaction, Lock.Type.WRITE, Lock.Status.BLOCK);
  }
  
  public Set<String> endTransaction(Transaction transaction) {
    Set<String> readVariables = transactionVariableReadLocks.get(transaction);
    Set<String> writeVariables = transactionVariableWriteLocks
        .get(transaction);
    Set<String> variables = new HashSet<String>();
    if( readVariables != null) {
      variables.addAll(readVariables);
      for( String readVariable : readVariables ) {
        readLocks.get(readVariable).remove(transaction);
      }
    }
    
    if( writeVariables != null ) {
      variables.addAll(writeVariables);
      for( String writeVariable : writeVariables ) {
        writeLocks.remove(writeVariable);
      }
    }
    
    transactionVariableReadLocks.remove(transaction);
    transactionVariableWriteLocks.remove(transaction);

    return variables;
  }
}
