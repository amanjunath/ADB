package nyu.edu.adbs.executor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import nyu.edu.adbs.locking.LockManager;
import nyu.edu.adbs.storage.Site;
import nyu.edu.adbs.storage.Variable;
import nyu.edu.adbs.transaction.Transaction;

public class SitesInitializer {
  
  private static SitesInitializer instance = new SitesInitializer();
  private static final int TOTAL_VARIABLES = 20;
  private static final int TOTAL_SITES = 10;
  
  private Map<Integer, Site> siteMap;
  private List<Site> sites;
  //private Set<String> variableNames;
  private LockManager lockManager;
  private Map<String, Set<Site>> variableSitesMap;
  
  private SitesInitializer() {
    
    Map<String, Transaction> readLockTable = new HashMap<String, Transaction>();
    Map<String, Set<Transaction>> writeLockTable
        = new HashMap<String, Set<Transaction>>();
    variableSitesMap = new HashMap<String,Set<Site>>();
    lockManager = new LockManager(readLockTable, writeLockTable);
    
    List<Map<String, Variable>> variableMapList 
        = new ArrayList<Map<String, Variable>>(TOTAL_SITES);
    
    for( int siteId = 1; siteId <= TOTAL_SITES; siteId++ ) {
      variableMapList.add(new HashMap<String, Variable>());
    }
    
    //variableNames = new HashSet<String>();
    for( int index = 1; index <= TOTAL_VARIABLES; ++index ) {
      Variable variable = new Variable(index, "X"+index, 10 * index);
      //variableNames.add("X"+index);
      
      if(index % 2 == 1 ) {
        int siteId = 1 + (index%TOTAL_SITES);
        Map<String, Variable> variableMap = variableMapList.get(siteId - 1);
        variableMap.put(variable.getVariableName(), new Variable(variable));
      }
      else {
        for( int siteId = 1; siteId <= TOTAL_SITES; siteId++ ) {
          Map<String, Variable> variableMap = variableMapList.get(siteId - 1);
          variableMap.put(variable.getVariableName(), new Variable(variable));
        }
      }
    }
    
    siteMap = new HashMap<Integer, Site>();
    sites = new ArrayList<Site>();
    for( int siteId = 1; siteId <= TOTAL_SITES; siteId++ ) {
      Site site = new Site(siteId, variableMapList.get(siteId - 1), 
          readLockTable, writeLockTable);
      siteMap.put(siteId, site);
      sites.add(site);
      for( String variable : variableMapList.get(siteId - 1).keySet() ) {
        Set<Site> sites = variableSitesMap.get(variable);
        if( sites == null ) {
          sites = new HashSet<Site>();
          variableSitesMap.put(variable, sites);
        }
        sites.add(site);
      }
    }
    
  }
  
  public static SitesInitializer getInstance() {
    return instance;
  }

  public Map<Integer, Site> getSiteMap() {
    return siteMap;
  }
  
  public List<Site> getSites() {
    return sites;
  }
  
  /*public Set<String> getVariableNames() {
    return variableNames;
  }*/
  
  public Map<String, Set<Site>> getVariableSitesMap() {
    return variableSitesMap;
  }
  
  public LockManager getLockManager() {
    return lockManager;
  }
  
}
