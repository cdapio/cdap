package com.continuuity.data.runtime.main;

import com.continuuity.app.guice.AppFabricServiceRuntimeModule;
import com.continuuity.app.guice.ProgramRunnerRuntimeModule;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.KafkaClientModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.guice.TwillModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.runtime.DaemonMain;
import com.continuuity.common.zookeeper.election.ElectionHandler;
import com.continuuity.common.zookeeper.election.LeaderElection;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.security.HBaseSecureStoreUpdater;
import com.continuuity.data.security.HBaseTokenUtils;
import com.continuuity.data2.util.hbase.HBaseTableUtilFactory;
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.internal.app.services.AppFabricServer;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.continuuity.test.internal.TempFolder;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.Credentials;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.common.ServiceListenerAdapter;
import org.apache.twill.yarn.YarnSecureStore;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Driver class for starting all reactor master services.
 * AppFabricHttpService
 * TwillRunnables: MetricsProcessor, MetricsHttp, LogSaver, TransactionService, StreamHandler.
 */
public class ReactorServiceMain extends DaemonMain {
  private static final Logger LOG = LoggerFactory.getLogger(ReactorServiceMain.class);

  private static final long MAX_BACKOFF_TIME_MS = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);
  private static final long SUCCESSFUL_RUN_DURATON_MS = TimeUnit.MILLISECONDS.convert(20, TimeUnit.MINUTES);

  public ReactorServiceMain(CConfiguration cConf, Configuration hConf, Map<String, Configuration> extraConfs) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.extraConfs = ImmutableMap.copyOf(extraConfs);
  }
  private boolean stopFlag = false;

  protected final CConfiguration cConf;
  protected final Configuration hConf;
  protected final Map<String, Configuration> extraConfs;
  private final Set<URI> resources = Sets.newHashSet();

  private final AtomicBoolean isLeader = new AtomicBoolean(false);

  private Injector baseInjector;
  private ZKClientService zkClientService;
  private LeaderElection leaderElection;
  private volatile TwillRunnerService twillRunnerService;
  private volatile TwillController twillController;
  private AppFabricServer appFabricServer;
  private MetricsCollectionService metricsCollectionService;

  private String serviceName;
  private TwillApplication twillApplication;
  private long lastRunTimeMs = System.currentTimeMillis();
  private int currentRun = 0;

  public static void main(final String[] args) throws Exception {
    LOG.info("Starting Reactor Service Main...");
    // todo move those lines somewhere else
    HiveConf hiveConf = new HiveConf();
    java.net.URL url = ReactorServiceMain.class.getClassLoader().getResource("hive-site.xml");
    LOG.info("Hive resource URL: {}", (url != null) ? url.toURI() : null);
//    ByteArrayOutputStream bout = new ByteArrayOutputStream();
//    PrintWriter printWriter = new PrintWriter(bout);
//    hiveConf.writeXml(printWriter);
//    LOG.info("Hiveconf = {}", bout.toString());
    String metastoreUris = hiveConf.get("hive.metastore.uris");
    LOG.info("Metastore URIs are: {}", metastoreUris);
    // todo do this checking somewhere...
    // Preconditions.checkNotNull(metastoreUris, "Metastore URIs not set in hive config.");
    Configuration newConf = new Configuration();
    newConf.clear();
    newConf.set("hive.metastore.uris", hiveConf.get("hive.metastore.uris"));
    newConf.set("hive.server2.thrift.port", "0");
    newConf.set("mapreduce.framework.name", "yarn");
//    newConf.set("hive.exec.local.scratchdir", "/tmp");
//    newConf.set("hive.querylog.location", "/tmp");
    new ReactorServiceMain(CConfiguration.create(), HBaseConfiguration.create(),
                           ImmutableMap.of("hive-site.xml", newConf)).doMain(args);
//    new ReactorServiceMain(CConfiguration.create(), HBaseConfiguration.create(),
//                           ImmutableMap.<String, Configuration>of()).doMain(args);
  }

  @Override
  public void init(String[] args) {
    twillApplication = createTwillApplication();
    if (twillApplication == null) {
      throw new IllegalArgumentException("TwillApplication cannot be null");
    }

    serviceName = twillApplication.configure().getName();

    baseInjector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new ZKClientModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new IOModule(),
      new AuthModule(),
      new KafkaClientModule(),
      new TwillModule(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new AppFabricServiceRuntimeModule().getDistributedModules(),
      new ProgramRunnerRuntimeModule().getDistributedModules(),
      new DataFabricModules(cConf, hConf).getDistributedModules(),
      new MetricsClientRuntimeModule().getDistributedModules()
    );
    // Initialize ZK client
    zkClientService = baseInjector.getInstance(ZKClientService.class);
  }

  @Override
  public void start() {
    zkClientService.startAndWait();

    leaderElection = new LeaderElection(zkClientService, "/election/" + serviceName, new ElectionHandler() {
      @Override
      public void leader() {
        LOG.info("Became leader.");
        Injector injector = baseInjector.createChildInjector();
        appFabricServer = injector.getInstance(AppFabricServer.class);
        appFabricServer.startAndWait();

        twillRunnerService = injector.getInstance(TwillRunnerService.class);
        twillRunnerService.startAndWait();
        scheduleSecureStoreUpdate(twillRunnerService);
        runTwillApps();
        isLeader.set(true);
      }

      @Override
      public void follower() {
        LOG.info("Became follower.");
        if (twillRunnerService != null) {
          twillRunnerService.stopAndWait();
        }
        if (appFabricServer != null) {
          appFabricServer.stopAndWait();
        }
        isLeader.set(false);
      }
    });
  }

  @Override
  public void stop() {
    LOG.info("Stopping {}", serviceName);
    stopFlag = true;
    if (isLeader.get() && twillController != null) {
      twillController.stopAndWait();
    }
    leaderElection.cancel();
    zkClientService.stopAndWait();
  }

  @Override
  public void destroy() {
  }

  private void addResource(URI uri) {
    resources.add(uri);
  }

  private TwillApplication createTwillApplication() {
    try {
      ImmutableMap.Builder<String, File> extraConfFiles = ImmutableMap.builder();
      for (Map.Entry<String, Configuration> extraConfEntry : extraConfs.entrySet()) {
        File conf = getSavedExtraConf(extraConfEntry.getKey(), extraConfEntry.getValue());
        extraConfFiles.put(extraConfEntry.getKey(), conf);
        addResource(conf.toURI());
      }
      return new ReactorTwillApplication(cConf, getSavedCConf(), getSavedHConf(), extraConfFiles.build());
    } catch (Exception e) {
      throw  Throwables.propagate(e);
    }
  }

  private void scheduleSecureStoreUpdate(TwillRunner twillRunner) {
    if (User.isHBaseSecurityEnabled(hConf)) {
      HBaseSecureStoreUpdater updater = new HBaseSecureStoreUpdater(hConf);
      twillRunner.scheduleSecureStoreUpdate(updater, 30000L, updater.getUpdateInterval(), TimeUnit.MILLISECONDS);
    }
  }


  private TwillPreparer prepare(TwillPreparer preparer) {
    return preparer.withDependencies(new HBaseTableUtilFactory().get().getClass())
      .addSecureStore(YarnSecureStore.create(HBaseTokenUtils.obtainToken(hConf, new Credentials())));
  }

  private void runTwillApps() {
    // If service is already running, return handle to that instance
    Iterable<TwillController> twillControllers = lookupService();
    Iterator<TwillController> iterator = twillControllers.iterator();

    if (iterator.hasNext()) {
      LOG.info("{} application is already running", serviceName);
      twillController = iterator.next();

      if (iterator.hasNext()) {
        LOG.warn("Found more than one instance of {} running. Stopping the others...", serviceName);
        for (; iterator.hasNext(); ) {
          TwillController controller = iterator.next();
          LOG.warn("Stopping one extra instance of {}", serviceName);
          controller.stopAndWait();
        }
        LOG.warn("Done stopping extra instances of {}", serviceName);
      }
    } else {
      LOG.info("Starting {} application", serviceName);
      TwillPreparer twillPreparer = getPreparer();
      twillController = twillPreparer.start();

      twillController.addListener(new ServiceListenerAdapter() {
        @Override
        public void failed(Service.State from, Throwable failure) {
          LOG.error("{} failed with exception... restarting with back-off.", serviceName, failure);
          backOffRun();
        }

        @Override
        public void terminated(Service.State from) {
          LOG.warn("{} got terminated... restarting with back-off", serviceName);
          backOffRun();
        }
      }, MoreExecutors.sameThreadExecutor());
    }
  }

  private File getSavedHConf() throws IOException {
    File hConfFile = saveHConf(hConf, File.createTempFile("hConf", ".xml"));
    hConfFile.deleteOnExit();
    return hConfFile;
  }

  private File getSavedCConf() throws IOException {
    File cConfFile = saveCConf(cConf, File.createTempFile("cConf", ".xml"));
    cConfFile.deleteOnExit();
    return cConfFile;
  }

  private File getSavedExtraConf(String confName, Configuration conf) throws IOException {
    TempFolder tempFolder = new TempFolder();
    File tFolder = tempFolder.newFolder("saved_confs");
    return saveExtraConf(conf, new File(tFolder, confName));
  }

  /**
   * Wait for sometime while looking up service in twill.
   */
  private Iterable<TwillController> lookupService() {
    int count = 100;
    Iterable<TwillController> iterable = twillRunnerService.lookup(serviceName);

    try {

      for (int i = 0; i < count; ++i) {
        if (iterable.iterator().hasNext()) {
          return iterable;
        }

        TimeUnit.MILLISECONDS.sleep(20);
      }
    } catch (InterruptedException e) {
      LOG.warn("Got interrupted exception: ", e);
      Thread.currentThread().interrupt();
    }

    return iterable;
  }

  private TwillPreparer getPreparer() {
    return prepare(twillRunnerService.prepare(twillApplication)
                     .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out))).withResources(resources)
    );
  }

  private void backOffRun() {
    if (stopFlag) {
      LOG.warn("Not starting a new run when stopFlag is true");
      return;
    }

    if (System.currentTimeMillis() - lastRunTimeMs > SUCCESSFUL_RUN_DURATON_MS) {
      currentRun = 0;
    }

    try {

      long sleepMs = Math.min(500 * (long) Math.pow(2, currentRun), MAX_BACKOFF_TIME_MS);
      LOG.info("Current restart run = {}. Backing off for {} ms...", currentRun, sleepMs);
      TimeUnit.MILLISECONDS.sleep(sleepMs);

    } catch (InterruptedException e) {
      LOG.warn("Got interrupted exception: ", e);
      Thread.currentThread().interrupt();
    }

    runTwillApps();
    ++currentRun;
    lastRunTimeMs = System.currentTimeMillis();
  }

  private static File saveHConf(Configuration conf, File file) throws IOException {
    Writer writer = Files.newWriter(file, Charsets.UTF_8);
    try {
      conf.writeXml(writer);
    } finally {
      writer.close();
    }
    return file;
  }

  private File saveCConf(CConfiguration conf, File file) throws IOException {
    Writer writer = Files.newWriter(file, Charsets.UTF_8);
    try {
      conf.writeXml(writer);
    } finally {
      writer.close();
    }
    return file;
  }

  private File saveExtraConf(Configuration conf, File file) throws IOException {
    Writer writer = Files.newWriter(file, Charsets.UTF_8);
    try {
      conf.writeXml(writer);
    } finally {
      writer.close();
    }
    return file;
  }

}
