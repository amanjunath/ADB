package nyu.edu.adbs.transaction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import nyu.edu.adbs.storage.Site;
import nyu.edu.adbs.storage.SiteManager;

public class ReadWriteTransaction extends Transaction {

  Map<String, Set<Site>> variableSitesMap;
  Map<String, Set<TimeSiteEntry>> variablesWritten;
  Map<String, Set<TimeSiteEntry>> variablesRead;
  //Map<String, Long> variablesLastAccessTime;
  
  private class TimeSiteEntry {
    private long time;
    private Site site;
    TimeSiteEntry(long time, Site site) {
      this.time = time;
      this.site = site;
    }
  }
  
  private SiteManager siteManager;
  
  public ReadWriteTransaction(String name, long timestamp, Type type,
      State state, Map<String, Set<Site>> variableSitesMap,
      SiteManager siteManager) {
    super(name, timestamp, type, state);
    this.variableSitesMap = variableSitesMap;
    this.variablesWritten = new HashMap<String, Set<TimeSiteEntry>>();
    this.variablesRead = new HashMap<String, Set<TimeSiteEntry>>();
    //this.variablesLastAccessTime = new HashMap<String, Long>();
    this.siteManager = siteManager;
  }

  @Override
  public Integer read(String variableName, long timestamp) {
    Integer readValue = null;
    Set<TimeSiteEntry> sitesRead = variablesRead.get(variableName);
    if( sitesRead == null ) {
      sitesRead = new HashSet<TimeSiteEntry>();
      variablesRead.put(variableName, sitesRead);
    }
    for( Site site : variableSitesMap.get(variableName) ) {
      if( site.isActive() && 
          !siteManager.isVariableDirtyAtSite(variableName, site)) {
        sitesRead.add(new TimeSiteEntry(timestamp, site));
        readValue = site.read(variableName);
        return readValue;
      }
    }
    return readValue;
    /*throw new IllegalStateException("Transaction "+this.getName()+": All sites "
        + "are down for variable " + variableName);*/
  }

  @Override
  public boolean write(String variableName, int value, long timestamp) {
    boolean write = false;
    Set<TimeSiteEntry> sitesModified = variablesWritten.get(variableName);
    if( sitesModified == null ) {
      sitesModified = new HashSet<TimeSiteEntry>();
      variablesWritten.put(variableName, sitesModified);
    }
    for( Site site : variableSitesMap.get(variableName) ) {
      if( site.isActive() ) {
        write = true;
        site.write(variableName, value);
        sitesModified.add(new TimeSiteEntry(timestamp, site));
      }
    }
   /* if( write ) {
      variablesLastAccessTime.put(variableName, timestamp);
    }*/
    return write;
  }

  @Override
  public void abort() {
    for(Map.Entry<String, Set<TimeSiteEntry>> variableSetsEntry 
        : variablesWritten.entrySet() ) {
      for( TimeSiteEntry timeSite : variableSetsEntry.getValue() ) {
        if( timeSite.site.isActive() ) {
          timeSite.site.rollback(variableSetsEntry.getKey());
        }
      }
    }
  }
  
  @Override
  public boolean commit() {
    for(Map.Entry<String, Set<TimeSiteEntry>> variableSetsEntry 
        : variablesWritten.entrySet() ) {
      for( TimeSiteEntry timeSite : variableSetsEntry.getValue() ) {
        if( !timeSite.site.isActive() || 
            ( timeSite.site.getLastFailedTime() != null && 
                timeSite.site.getLastFailedTime()  > timeSite.time )) {
          return false;
        }
      }
    }
    for(Map.Entry<String, Set<TimeSiteEntry>> variableSetsEntry 
        : variablesRead.entrySet() ) {
      for( TimeSiteEntry timeSite : variableSetsEntry.getValue() ) {
        if( !timeSite.site.isActive() || 
            ( timeSite.site.getLastFailedTime() != null && 
                timeSite.site.getLastFailedTime()  > timeSite.time )) {
          return false;
        }
      }
    }
    for(Map.Entry<String, Set<TimeSiteEntry>> variableSetsEntry 
        : variablesWritten.entrySet() ) {
      Set<Site> dirtySites = new HashSet<Site>
          (variableSitesMap.get(variableSetsEntry.getKey()));
      for( TimeSiteEntry timeSite : variableSetsEntry.getValue() ) {
    	  dirtySites.remove(timeSite.site);
        timeSite.site.commit(variableSetsEntry.getKey());
      }
      siteManager.updateVariableSiteMap(variableSetsEntry.getKey(), dirtySites);
    }
    return true;
  }

}
