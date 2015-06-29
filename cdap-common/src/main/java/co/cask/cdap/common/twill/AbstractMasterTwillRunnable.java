/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.common.twill;

import co.cask.cdap.common.conf.CConfiguration;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.apache.twill.internal.Services;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Abstract TwillRunnable class for Master system service.
 */
public abstract class AbstractMasterTwillRunnable extends AbstractTwillRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractMasterTwillRunnable.class);

  protected String name;
  private String cConfName;
  private String hConfName;
  private Configuration hConf;
  private CConfiguration cConf;
  private List<Service> services;
  private volatile Thread runThread;

  public AbstractMasterTwillRunnable(String name, String cConfName, String hConfName) {
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

      LOG.debug("{} cConf {}", name, cConf);
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
    SettableFuture<String> completionFuture = SettableFuture.create();
    for (Service service : services) {
      service.addListener(createServiceListener(service.getClass().getName(), completionFuture),
          Threads.SAME_THREAD_EXECUTOR);
    }

    Services.chainStart(services.get(0), services.subList(1, services.size()).toArray(new Service[0]));
    LOG.info("Runnable started {}", name);


    try {
      // exit as soon as any service completes
      completionFuture.get();
    } catch (InterruptedException e) {
      LOG.debug("Waiting on latch interrupted {}", name);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      throw Throwables.propagate(e.getCause());
    }

    List<Service> reverse = Lists.reverse(services);
    Services.chainStop(reverse.get(0), reverse.subList(1, reverse.size()).toArray(new Service[0]));

    LOG.info("Runnable stopped {}", name);
  }

  private Service.Listener createServiceListener(final String name, final SettableFuture<String> future) {
    return new ServiceListenerAdapter() {
      @Override
      public void terminated(Service.State from) {
        LOG.info("Service " + name + " terminated");
        future.set(name);
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        LOG.error("Service " + name + " failed", failure);
        future.setException(failure);
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
   * Class extending AbstractMasterTwillRunnable should populate services
   * with a list of Services which will be started in increasing order of index.
   * The services will be stopped in the reverse order.
   */
  protected abstract void getServices(List<? super Service> services);

  /**
   * Performs initialization task.
   */
  protected abstract void doInit(TwillContext context);
}
