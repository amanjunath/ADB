package nyu.edu.adbs.transaction;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import nyu.edu.adbs.locking.Lock;
import nyu.edu.adbs.locking.Lock.Status;
import nyu.edu.adbs.locking.LockManager;
import nyu.edu.adbs.locking.WriteOperation;
import nyu.edu.adbs.storage.Site;
import nyu.edu.adbs.storage.SiteManager;

public class TransactionManager {
  
  private Map<String, Transaction> transactionMap;
  private Map<Integer, Site> siteMap;
  private Map<String, Set<Site>> variableSitesMap;
  private LinkedHashMap<String, LinkedHashMap<Transaction, Lock>> 
      blockedTransactions;
  private List<Site> sites;
  private OutputStream outputStream;
  private LockManager lockManager;
  private SiteManager siteManager;

  private void writeToStreamln(String output) {
    try {
      output = output + "\n";
      outputStream.write(output.getBytes());
    } catch (IOException e) {
    }
  }
  
  public TransactionManager(Map<Integer, Site> siteMap, 
      Map<String, Set<Site>> variableSitesMap, LockManager lockManager,
      List<Site> sites, OutputStream outputStream) {
    this.transactionMap = new HashMap<String, Transaction>();
    this.siteMap = siteMap;
    this.variableSitesMap = variableSitesMap;
    this.outputStream = outputStream;
    this.lockManager = lockManager;
    this.blockedTransactions = new 
        LinkedHashMap<String, LinkedHashMap<Transaction, Lock>>();
    this.sites = sites;
    this.siteManager = new SiteManager();
  }
  
  public void beginRO(String transactionName, long startTime) {
    if( transactionMap.containsKey(transactionName) ) {
      writeToStreamln("Transaction with same name already"
          + "exists");
      return;
    }
    
    String output = new StringBuilder("Read only Transaction ")
        .append(transactionName).append(" started").toString(); 
    writeToStreamln(output);
    
    //Get a copy of all the variables
    Map<String, Integer> variableValuesOld = new HashMap<String, Integer>();
    boolean variableFound = false;
    for( String variableName : variableSitesMap.keySet() ) {
      for( Site site : variableSitesMap.get(variableName) ) {
        if( site.isActive() ) {
          variableValuesOld.put(variableName, site.readOld(variableName));
          variableFound = true;
          break;
        }
      }
      if( !variableFound ) {
        //print in output that this read operation will not be possible for this
        //variable.
      }
    }
    
    transactionMap.put(transactionName, new ReadOnlyTransaction
        (transactionName, startTime, Transaction.Type.READ_ONlY, 
            Transaction.State.ACTIVE, variableValuesOld) );
  }
  
  public void begin(String transactionName, long startTime) {
    if( transactionMap.containsKey(transactionName) ) {
      writeToStreamln("Transaction with same name already"
          + "exists");
      return;
    }
    String output = new StringBuilder("Read/Write Transaction ")
        .append(transactionName).append(" started").toString(); 
    writeToStreamln(output);

    transactionMap.put(transactionName, new ReadWriteTransaction
        (transactionName, startTime, Transaction.Type.READ_WRITE, 
            Transaction.State.ACTIVE, variableSitesMap, siteManager));
  }
  
  public void end(String transactionName, long timestamp) {
    Transaction transaction = transactionMap.get(transactionName);
    if( transaction == null ) {
      writeToStreamln("Illegal transaction name or transac"
          + "tion already ended");
      return;
    }
    
    if( transaction.commit() ) {
      String output = new StringBuilder("Transaction ")
        .append(transactionName).append(" ended").toString(); 
        writeToStreamln(output);
    }
    else {
      String output = new StringBuilder("Transaction ")
        .append(transactionName).append(" aborted").toString(); 
      writeToStreamln(output);
      abort(transaction, timestamp);
    }
    removeLocksAndUnblock(transaction, timestamp);
  }
  
  public void read(String transactionName, String variableName,
      long timestamp) {
    Transaction transaction = transactionMap.get(transactionName);
    if( transaction == null ) {
      writeToStreamln("Illegal transaction name");
      return;
    }
    if( transaction.getType() == Transaction.Type.READ_ONlY ) {
      //try {
        Integer variableValue = transaction.read(variableName, timestamp);
        if( variableValue != null ) {
         String output = new StringBuilder("Read only Transaction ")
             .append(transactionName).append(" read variable ")
                 .append(variableName).append(" = ").append(variableValue)
                     .toString(); 
         writeToStreamln(output);
        }
        else {
          String output = new StringBuilder("Read only Transaction ")
              .append(transactionName).append(" could not read variable ")
                  .append(variableName).append(" because"
                      + " all sites are down.").toString();
          writeToStreamln(output);
          output = new StringBuilder("Transaction ")
              .append(transactionName).append(" aborted").toString(); 
                  writeToStreamln(output);
          abort(transaction, timestamp);
        }
     // }
     /* catch(NullPointerException exception) {
        String output = new StringBuilder("Variable ")
        .append(variableName).append(" could not be read because each site co"
            + "ntaining the variable was down before the transaction")
                .toString(); 
        writeToStreamln(output);
      }*/
    }
    
    else {
      Lock lock = lockManager.acquireRead(transaction, variableName);
      Status lockStatus = lock.getStatus();
      
      //if giving the lock to a transaction then check for blocked transactions
      if( lockStatus == Status.ACCEPT ) {
        Integer variableValue = transaction.read(variableName, timestamp);
        if( variableValue != null ) {
        String output = new StringBuilder("Read/Write Transaction ")
            .append(transactionName).append(" read variable ")
                .append(variableName).append(" = ").append(variableValue)
                    .toString();
        writeToStreamln(output);
        }
        else {
          String output = new StringBuilder("Read/Write Transaction ")
              .append(transactionName).append(" could not read variable ")
                  .append(variableName).append(" because"
                      + " all sites are down.").toString(); 
          writeToStreamln(output);
          output = new StringBuilder("Transaction ")
              .append(transactionName).append(" aborted").toString(); 
          writeToStreamln(output);
          abort(transaction, timestamp);
        }
      }
      
      //if status is blocked then make sure that you check the blocked transa
      //ctions and abort your transaction if some earlier transaction is wa-
      //iting for the same lock.
      else if( lockStatus == Status.BLOCK ) {
        LinkedHashMap<Transaction, Lock> transactionLockMap 
            = blockedTransactions.get(variableName);
        if( transactionLockMap == null ) {
          transactionLockMap = new LinkedHashMap<Transaction, Lock>();
          blockedTransactions.put(variableName, transactionLockMap);
        }
        //iterate on transactionLockMap; for every transaction, compare transaction's timestamps
        if( transactionLockMap.containsKey(transaction)) {
          String output = "Blocked transaction can be blocked"
              + "on just one operation";
          writeToStreamln(output);
          return;
        }
        
        transactionLockMap.put(transaction, lock);

        String output = new StringBuilder("Read/Write Transaction ")
            .append(transactionName).append(" blocked.").toString();
        writeToStreamln(output);
      }
      else {
        String output = new StringBuilder("Read/Write Transaction ")
            .append(transactionName).append(" aborted by wait-die.").toString();
        writeToStreamln(output);
        abort(transaction, timestamp);
      }
    }
  }

  public void write(String transactionName, String variableName, 
      int value, long timestamp) {
    Transaction transaction = transactionMap.get(transactionName);
    if( transaction != null && transaction.getType() 
        == Transaction.Type.READ_WRITE) {
      Lock lock = lockManager
          .acquireWrite(transaction, variableName);
      Status lockStatus = lock.getStatus();
      if( lockStatus == Status.ACCEPT ) {
        if( transaction.write(variableName, value, timestamp) ) {
          String output = new StringBuilder("Read/Write Transaction ")
              .append(transactionName).append(" writing variable ")
                  .append(variableName).append(" = ").append(value)
                      .toString();
          writeToStreamln(output);
        }
        else {
          String output = new StringBuilder("Read/Write Transaction ")
              .append(transactionName).append(" unable to write variable ")
                  .append(variableName).append(" because all sites are down ")
                      .toString();
          writeToStreamln(output);
          output = new StringBuilder("Transaction ")
              .append(transactionName).append(" aborted").toString(); 
          writeToStreamln(output);
          abort(transaction, timestamp);
        }
      }
      else if( lockStatus == Status.BLOCK ) {
        LinkedHashMap<Transaction, Lock> transactionLockMap 
            = blockedTransactions.get(variableName);
        if( transactionLockMap == null ) {
          transactionLockMap = new LinkedHashMap<Transaction, Lock>();
          blockedTransactions.put(variableName, transactionLockMap);
        }
        
        if( transactionLockMap.containsKey(transaction)) {
          writeToStreamln("Blocked transaction can be blocked"
              + "on just one operation");
          return;
        }
        lock.setWriteOperation(new WriteOperation(variableName, value));
        transactionLockMap.put(transaction, lock);
        String output = new StringBuilder("Read/Write Transaction ")
            .append(transactionName).append(" blocked.").toString();
        writeToStreamln(output);
      }
      else {
        String output = new StringBuilder("Read/Write Transaction ")
          .append(transactionName).append(" aborted by wait-die.").toString();
        writeToStreamln(output);
        abort(transaction, timestamp);
      }
    }
  }
  
  public void dump() {
    for( Site site : sites ) {
      writeToStreamln(site.getSiteId()+": "+site.getVariables());
    }
  }
  
  public void dump(int siteId) {
    Site site = siteMap.get(siteId);
    writeToStreamln(site.getSiteId()+": "+site.getVariables());
  }
  
  public void fail(int siteId, long timestamp) {
    Site site = siteMap.get(siteId);
    if( site != null ) {
      site.fail(timestamp);
      String output = new StringBuilder("Site ").append(siteId)
          .append(" failed.").toString();
      writeToStreamln(output);
    }
  }
  
  public void recover(int siteId) {
    Site site = siteMap.get(siteId);
    if( site != null ) {
      site.recover();
      String output = new StringBuilder("Site ").append(siteId)
          .append(" recovered.").toString();
      writeToStreamln(output);
    }
  }
  
  private void abort(Transaction transaction, long timestamp) {
    if( transaction.getType() == Transaction.Type.READ_WRITE ) {
      transaction.abort();
      removeLocksAndUnblock(transaction, timestamp);
    }
  }
  
  private void removeLocksAndUnblock(Transaction transaction, 
      long timestamp) {
    Set<String> variables = lockManager.endTransaction(transaction);
    transactionMap.remove(transaction.getName());
    for(LinkedHashMap<Transaction, Lock> transactionLockMap 
        : blockedTransactions.values() ) {
      transactionLockMap.remove(transaction);
    }
    
    for( Map.Entry<String, LinkedHashMap<Transaction, Lock>> blockedTranEntry 
        : blockedTransactions.entrySet() ) {
      LinkedHashMap<Transaction, Lock> transactionLockMap = blockedTranEntry
          .getValue();
      String variable = blockedTranEntry.getKey();
      if( variables.contains(variable) ) {
        Iterator<Map.Entry<Transaction, Lock>> iterator 
            = transactionLockMap.entrySet().iterator();
        if( iterator.hasNext() ) 
        {
          Map.Entry<Transaction, Lock> entry = iterator.next();
          Transaction blockedTransaction = entry.getKey();
          Lock lock = entry.getValue();
          iterator.remove();
          if( lock.getType() == Lock.Type.READ ) {
            read(blockedTransaction.getName(), variable, timestamp);
          }
          else if( lock.getType() == Lock.Type.WRITE ) {
            write(blockedTransaction.getName(), variable, 
                lock.getWriteOperation().getNewValue(), timestamp);
          }
        }
      }
    }
  }

}
