package org.apache.zeppelin.eventhook;

/**
 * Created by shim on 2016. 11. 3..
 */
public interface ZeppelinEventHook {
  void onServerInit();
  void onServerShutdownStart();
  void onNoteStart();
  void onNoteFinished();
}
