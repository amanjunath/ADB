package nyu.edu.adbs.executor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;

import nyu.edu.adbs.storage.Site;
import nyu.edu.adbs.transaction.TransactionManager;

public class TransactionExecutor {
  
  private Map<Integer, Site> siteMap;
  private TransactionManager transactionManager;
  private File inputFile;
  //private File outputFile;
  
  public TransactionExecutor(File inputFile, File outputFile) {
    this.inputFile = inputFile;
    //this.outputFile = outputFile;
    siteMap = SitesInitializer.getInstance().getSiteMap();
    //transactionManager = new TransactionManager(new FileOutputStream(outputFile));
    transactionManager = new TransactionManager(siteMap, 
        SitesInitializer.getInstance().getVariableSitesMap(), 
        SitesInitializer.getInstance().getLockManager(), 
        SitesInitializer.getInstance().getSites(), System.out);
  }
  
  public void start() {

    try {
      BufferedReader bufferedReader = new BufferedReader
          (new FileReader(inputFile));
      String commandLine = bufferedReader.readLine();
      long timestamp = 0;
      
      while(commandLine != null) {
        System.out.println(commandLine);
        String[] commands = commandLine.split(";");
        for( String command : commands ) {
          String trimmedCommand = command.trim();
          String function = trimmedCommand.substring
              (0, trimmedCommand.indexOf("(")).toUpperCase().trim();
          String input = trimmedCommand.substring
              (trimmedCommand.indexOf("(") + 1, trimmedCommand.indexOf(")"))
                  .toUpperCase().trim();
          switch(function) {
            case "BEGIN" : {
              String transactionName = input;
              transactionManager.begin(transactionName, timestamp);
              break;
            }
            case "BEGINRO" : {
              String transactionName = input;
              transactionManager.beginRO(transactionName, timestamp);
              break;
            }
            case "END" : {
              String transactionName = input;
              transactionManager.end(transactionName, timestamp);
              break;
            }
            case "FAIL" : {
              int siteId = Integer.parseInt(input);
              transactionManager.fail(siteId, timestamp);
              break;
            }
            case "RECOVER" : {
              int siteId = Integer.parseInt(input);
              transactionManager.recover(siteId);
              break;
            }
            case "W" : {
              String [] parameters = input.split(",");
              String transactionName = parameters[0].trim();
              String variableName = parameters[1].trim();
              int newValue = Integer.parseInt(parameters[2].trim());
              transactionManager.write(transactionName, variableName, 
                  newValue, timestamp);
              break;
            }
            case "R" : {
              String [] parameters = input.split(",");
              String transactionName = parameters[0].trim();
              String variableName = parameters[1].trim();
              transactionManager.read(transactionName, variableName, timestamp);
              break;
            }
            case "DUMP" : {
              if( input.equals("") ) {
                transactionManager.dump();
              }
              else {
                transactionManager.dump(Integer.parseInt(input));
              }
              break;
            }
          }
        }
        commandLine = bufferedReader.readLine();
        timestamp++;
      }
      bufferedReader.close();
    }
    catch(Exception e) {
      e.printStackTrace();
    }
    
  }

  public static void main(String args[]) {
    TransactionExecutor transactionExecutor = 
        new TransactionExecutor(new File("input.txt"), new File("output.txt"));
    transactionExecutor.start();
  }

}
