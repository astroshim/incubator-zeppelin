package org.apache.zeppelin.eventhook;

/**
 * Created by shim on 2016. 10. 30..
 */
public interface Filter {
  public void execute(String request);
}
