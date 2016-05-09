package co.cask.cdap.logging.read;

import java.util.ArrayList;

/**
 *
 */
public class CollectingCallback implements Callback {
  private ArrayList<LogEvent> logEvents;

  @Override
  public void init() {
    logEvents = new ArrayList<>();
  }

  @Override
  public void handle(LogEvent event) {
    logEvents.add(event);
  }

  @Override
  public int getCount() {
    return logEvents.size();
  }

  @Override
  public void close() {

  }

  public ArrayList<LogEvent> getLogEvents() {
    return logEvents;
  }
}
