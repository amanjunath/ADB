package nyu.edu.adbs.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SiteManager {

  private Map<String, Set<Site>> dirtyVariableSiteMap 
      = new HashMap<String, Set<Site>>();
  
  public void updateVariableSiteMap(String variable, Set<Site> sites) {
    dirtyVariableSiteMap.put(variable, sites);
  }
  
  public boolean isVariableDirtyAtSite(String variable, Site site ) {
    if( dirtyVariableSiteMap.containsKey(variable) ) {
      return dirtyVariableSiteMap.get(variable).contains(site);
    }
    return false;
  }
  
}
