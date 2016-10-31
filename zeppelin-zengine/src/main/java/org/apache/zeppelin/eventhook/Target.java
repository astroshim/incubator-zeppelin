package org.apache.zeppelin.eventhook;

/**
 * Created by shim on 2016. 10. 30..
 */
public class Target {
  public void execute(String request){
    System.out.println("Executing request: " + request);
  }
}
