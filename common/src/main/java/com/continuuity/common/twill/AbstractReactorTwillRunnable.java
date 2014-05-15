package com.continuuity.common.twill;

import com.continuuity.common.conf.CConfiguration;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.common.ServiceListenerAdapter;
import org.apache.twill.common.Services;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Abstract TwillRunnable class for Reactor YARN services.
 */
public abstract class AbstractReactorTwillRunnable extends AbstractTwillRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractReactorTwillRunnable.class);

  protected String name;
  private String cConfName;
  private String hConfName;
  private Configuration hConf;
  private CConfiguration cConf;
  private List<Service> services;
  private volatile Thread runThread;

  public AbstractReactorTwillRunnable(String name, String cConfName, String hConfName) {
    this.name = name;
    this.cConfName = cConfName;
    this.hConfName = hConfName;
  }

  @Override
  public TwillRunnableSpecification configure() {
    return TwillRunnableSpecification.Builder.with()
      .setName(name)
      .withConfigs(ImmutableMap.of("cConf", cConfName, "hConf", hConfName))
      .build();
  }

  @Override
  public final void initialize(TwillContext context) {
    super.initialize(context);

    name = context.getSpecification().getName();
    Map<String, String> configs = context.getSpecification().getConfigs();

    try {
      // Load configuration
      hConf = new Configuration();
      hConf.clear();
      hConf.addResource(new File(configs.get("hConf")).toURI().toURL());

      UserGroupInformation.setConfiguration(hConf);

      cConf = CConfiguration.create();
      cConf.clear();
      cConf.addResource(new File(configs.get("cConf")).toURI().toURL());

      LOG.debug("{} Continuuity conf {}", name, cConf);
      LOG.debug("{} HBase conf {}", name, hConf);

      doInit(context);

      services = Lists.newArrayList();
      getServices(services);
      Preconditions.checkArgument(!services.isEmpty(), "Should have at least one service");

    } catch (Throwable t) {
      throw Throwables.propagate(t);
    }
  }

  @Override
  public void run() {
    runThread = Thread.currentThread();

    LOG.info("Starting runnable {}", name);
    List<ListenableFuture<Service.State>> completions = Lists.newArrayList();
    for (Service service : services) {
      SettableFuture<Service.State> completion = SettableFuture.create();
      service.addListener(createServiceListener(completion), Threads.SAME_THREAD_EXECUTOR);
      completions.add(completion);
    }

    Services.chainStart(services.get(0), services.subList(1, services.size()).toArray(new Service[0]));
    LOG.info("Runnable started {}", name);


    try {
      Futures.allAsList(completions).get();
    } catch (InterruptedException e) {
      LOG.debug("Waiting on latch interrupted {}", name);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      LOG.error("Exception in service.", e);
      throw Throwables.propagate(e);
    }

    List<Service> reverse = Lists.reverse(services);
    Services.chainStop(reverse.get(0), reverse.subList(1, reverse.size()).toArray(new Service[0]));

    LOG.info("Runnable stopped {}", name);
  }

  private Service.Listener createServiceListener(final SettableFuture<Service.State> completion) {
    return new ServiceListenerAdapter() {
      @Override
      public void terminated(Service.State from) {
        completion.set(from);
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        completion.setException(failure);
      }
    };
  }

  protected final Configuration getConfiguration() {
    return hConf;
  }

  protected final CConfiguration getCConfiguration() {
    return cConf;
  }

  @Override
  public void stop() {
    if (runThread != null) {
      runThread.interrupt();
    }
  }

  /**
   * Class extending AbstractReactorTwillRunnable should populate services
   * with a list of Services which will be started in increasing order of index.
   * The services will be stopped in the reverse order.
   */
  protected abstract void getServices(List<? super Service> services);

  /**
   * Performs initialization task.
   */
  protected abstract void doInit(TwillContext context);
}
