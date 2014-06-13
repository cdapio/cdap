package com.continuuity.data.runtime.main;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RunId;
import org.apache.twill.api.SecureStoreUpdater;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.common.Cancellable;
import org.apache.twill.internal.SingleRunnableApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class InMemoryTwillRunnerService extends AbstractIdleService implements TwillRunnerService {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryTwillRunnerService.class);
  private final Table<String, RunId, InMemoryTwillController> controllers;
  private Iterable<LiveInfo> liveInfos;

  private static final Function<InMemoryTwillController, TwillController> CAST_CONTROLLER =
    new Function<InMemoryTwillController, TwillController>() {
    @Override
    public TwillController apply(InMemoryTwillController controller) {
      return controller;
    }
  };

  public InMemoryTwillRunnerService() {
    this.controllers = HashBasedTable.create();
  }

  @Override
  protected void startUp() throws Exception {
    liveInfos = createLiveInfos();
  }

  @Override
  protected void shutDown() throws Exception {

  }

  @Override
  public TwillPreparer prepare(TwillRunnable runnable) {
    return prepare(runnable, ResourceSpecification.BASIC);
  }

  @Override
  public TwillPreparer prepare(TwillRunnable runnable, ResourceSpecification resourceSpecification) {
    return prepare(new SingleRunnableApplication(runnable, resourceSpecification));
  }

  @Override
  public TwillPreparer prepare(TwillApplication application) {
    Preconditions.checkState(isRunning(), "Service not start. Please call start() first.");
    final TwillSpecification twillSpec = application.configure();
    final String appName = twillSpec.getName();

    return new InMemoryTwillPreparer(twillSpec, new InMemoryTwillControllerFactory() {
      @Override
      public InMemoryTwillController create(RunId runId, Iterable<LogHandler> logHandlers) {
        InMemoryTwillController controller = new InMemoryTwillController();
        synchronized (InMemoryTwillRunnerService.this) {
          Preconditions.checkArgument(!controllers.contains(appName, runId),
                                      "Application %s with runId %s is already running.", appName, runId);
          controllers.put(appName, runId, controller);
        }
        return controller;
      }
    });
  }

  @Override
  public TwillController lookup(String applicationName, RunId runId) {
    return controllers.get(applicationName, runId);
  }

  @Override
  public Iterable<TwillController> lookup(final String applicationName) {
    return new Iterable<TwillController>() {
      @Override
      public Iterator<TwillController> iterator() {
        return Iterators.transform(ImmutableList.copyOf(controllers.row(applicationName).values()).iterator(),
                                   CAST_CONTROLLER);
      }
    };
  }

  @Override
  public Iterable<LiveInfo> lookupLive() {
    return liveInfos;
  }

  @Override
  public Cancellable scheduleSecureStoreUpdate(SecureStoreUpdater updater, long initialDelay, long delay, TimeUnit unit) {
    return new Cancellable() {
      @Override
      public void cancel() {
        //No-op
      }
    };
  }

  private Iterable<LiveInfo> createLiveInfos() {
    return new Iterable<LiveInfo>() {

      @Override
      public Iterator<LiveInfo> iterator() {
        Map<String, Map<RunId, InMemoryTwillController>> controllerMap = ImmutableTable.copyOf(controllers).rowMap();
        return Iterators.transform(controllerMap.entrySet().iterator(),
                                   new Function<Map.Entry<String, Map<RunId, InMemoryTwillController>>, LiveInfo>() {
          @Override
          public LiveInfo apply(final Map.Entry<String, Map<RunId, InMemoryTwillController>> entry) {
            return new LiveInfo() {
              @Override
              public String getApplicationName() {
                return entry.getKey();
              }

              @Override
              public Iterable<TwillController> getControllers() {
                return Iterables.transform(entry.getValue().values(), CAST_CONTROLLER);
              }
            };
          }
        });
      }
    };
  }
}
