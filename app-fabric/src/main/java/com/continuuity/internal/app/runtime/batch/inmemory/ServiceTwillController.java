package com.continuuity.internal.app.runtime.batch.inmemory;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.twill.api.Command;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;

import java.util.Iterator;
import java.util.concurrent.Executor;

/**
 *
 */
public class ServiceTwillController extends AbstractIdleService implements TwillController {

  ServiceTwillController(String appName) {

  }
  @Override
  public void addLogHandler(LogHandler logHandler) {

  }

  @Override
  public ServiceDiscovered discoverService(String s) {
   return new ServiceDiscovered() {
      @Override
      public String getName() {
        return null;
      }

      @Override
      public Cancellable watchChanges(ChangeListener changeListener, Executor executor) {
        return null;
      }

      @Override
      public boolean contains(Discoverable discoverable) {
        return false;
      }

      @Override
      public Iterator<Discoverable> iterator() {
        return null;
      }
    };
  }

  @Override
  public ListenableFuture<Integer> changeInstances(String s, int i) {
    // create a future to change the instances of runnable id'ed by 's' and send that
    return null;
  }

  @Override
  public ResourceReport getResourceReport() {
    //no resource report for single node
    return null;
  }

  @Override
  public RunId getRunId() {
    return null;
  }

  @Override
  public ListenableFuture<Command> sendCommand(Command command) {
    return null;
  }

  @Override
  public ListenableFuture<Command> sendCommand(String s, Command command) {
    return null;
  }

  @Override
  public void kill() {
    // kill all the threads
  }

  @Override
  protected void startUp() throws Exception {
    // start all the runnables

  }

  @Override
  protected void shutDown() throws Exception {
     // shutdown all the runnables
  }

}
