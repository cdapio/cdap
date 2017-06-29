package co.cask.cdap.internal.app.services;

import co.cask.cdap.app.store.RuntimeStore;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Service that receives program statuses and persists to the store
 */
public class ProgramStatusService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramStatusService.class);
  private final RuntimeStore runtimeStore;
  private final ExecutorService taskExecutorService;
  private volatile boolean stopping = false;

  @Inject
  public ProgramStatusService(RuntimeStore runtimeStore) {
    this.runtimeStore = runtimeStore;
    this.taskExecutorService =
      Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("scheduler-subscriber-task-%d").build());
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting ProgramStatusService");

    taskExecutorService.submit(new ProgramStatusSubscriberThread());
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping ProgramStatusService");
    stopping = true;
  }

  private class ProgramStatusSubscriberThread implements Runnable {
    @Override
    public void run() {
      while (!stopping) {

      }
    }
  }
}
