/*
 * Copyright © 2015-2023 Cask Data, Inc.
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

package io.cdap.cdap.data.runtime.main;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.multibindings.MapBinder;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.zookeeper.election.LeaderElectionInfoService;
import io.cdap.cdap.data2.datafabric.dataset.service.DatasetService;
import io.cdap.cdap.internal.app.services.AppFabricServer;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.security.authorization.AccessControllerInstantiator;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.api.ElectionHandler;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.internal.zookeeper.LeaderElection;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link MasterServiceMain}.
 */
public class MasterServiceMainTest {

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testInjector() {
    Injector baseInjector =
        MasterServiceMain.createProcessInjector(CConfiguration.create(), new Configuration());
    Injector injector = MasterServiceMain.createLeaderInjector(
        baseInjector.getInstance(CConfiguration.class),
        baseInjector.getInstance(Configuration.class),
        baseInjector.getInstance(ZKClientService.class),
        new LeaderElectionInfoService(
            baseInjector.getInstance(ZKClientService.class), "/election"));

    Assert.assertNotNull(injector.getInstance(AccessControllerInstantiator.class));
    Assert.assertNotNull(injector.getInstance(DatasetService.class));
    Assert.assertNotNull(injector.getInstance(AppFabricServer.class));
  }

  @Test
  public void test() {
    Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        MapBinder<String, String> mapBinder = MapBinder.newMapBinder(
            binder(), String.class, String.class,
            Constants.Dataset.Manager.DefaultDatasetModules.class);
        mapBinder.addBinding("key1").toInstance("value1");
      }
    }, new AbstractModule() {
      @Override
      protected void configure() {
        MapBinder<String, String> mapBinder = MapBinder.newMapBinder(
            binder(), String.class, String.class,
            Constants.Dataset.Manager.DefaultDatasetModules.class);
        mapBinder.addBinding("key2").toInstance("value2");
      }
    });
  }

  @Test
  public void testStartAndStopFollowerAndCloseQuietly() throws Exception {
    InMemoryZKServer zkServer = InMemoryZKServer.builder().build();
    zkServer.startAsync().awaitRunning();
    try {
      ZKClientService preLeaderZk = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
      preLeaderZk.startAsync().awaitRunning();
      try {
        CountDownLatch leaderElected = new CountDownLatch(1);
        LeaderElection primaryLeader = new LeaderElection(
            preLeaderZk,
            "/election/" + Constants.Service.MASTER_SERVICES,
            new ElectionHandler() {
              @Override
              public void leader() {
                leaderElected.countDown();
              }

              @Override
              public void follower() {
              }
            });
        primaryLeader.startAsync().awaitRunning();
        try {
          Assert.assertTrue(leaderElected.await(10, TimeUnit.SECONDS));

          File localDataDir = tempFolder.newFolder("localData");
          File hdfsDir = tempFolder.newFolder("hdfsNs");
          CConfiguration siteConf = CConfiguration.create();
          siteConf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());
          siteConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir.getAbsolutePath());
          siteConf.set(Constants.CFG_HDFS_NAMESPACE, hdfsDir.getAbsolutePath());

          LogAppenderInitializer failingAppender = Mockito.mock(LogAppenderInitializer.class);
          Mockito.doAnswer(inv -> sneakyThrow(new IOException("simulated close error")))
              .when(failingAppender).close();

          runWithSiteConf(siteConf, () -> {
            MasterServiceMain master = new MasterServiceMain();
            setField(master, "logAppenderInitializer", failingAppender);
            master.init(new String[0]);
            master.start();
            master.stop();
            master.destroy();
          });

          Mockito.verify(failingAppender).initialize();
          Mockito.verify(failingAppender).close();
        } finally {
          primaryLeader.stopAsync().awaitTerminated();
        }
      } finally {
        preLeaderZk.stopAsync().awaitTerminated();
      }
    } finally {
      zkServer.stopAsync().awaitTerminated();
    }
  }

  @Test
  public void testGetAndStartAndStopQuietlyHelpers() throws Exception {
    TestIdleService idleService = new TestIdleService(false);
    Injector injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        bind(TestIdleService.class).toInstance(idleService);
      }
    });

    Method getAndStart =
        MasterServiceMain.class.getDeclaredMethod("getAndStart", Injector.class, Class.class);
    getAndStart.setAccessible(true);
    TestIdleService started =
        (TestIdleService) getAndStart.invoke(null, injector, TestIdleService.class);
    Assert.assertSame(idleService, started);
    Assert.assertEquals(Service.State.RUNNING, started.state());

    Method stopQuietly = MasterServiceMain.class.getDeclaredMethod("stopQuietly", Service.class);
    stopQuietly.setAccessible(true);
    stopQuietly.invoke(null, new Object[]{null});
    stopQuietly.invoke(null, started);
    Assert.assertEquals(Service.State.TERMINATED, started.state());

    TestIdleService failingStopService = new TestIdleService(true);
    failingStopService.startAsync().awaitRunning();
    stopQuietly.invoke(null, failingStopService);
    Assert.assertEquals(Service.State.FAILED, failingStopService.state());
  }

  @Test
  public void testCreateDirectoryLoginAndLeaderElectionCleanup() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    File localDir = tempFolder.newFolder("masterData");
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, localDir.getAbsolutePath());

    MasterServiceMain master = createMasterWithConf(cConf);

    // 1. createDirectory(FileContext, String) propagates generic IOException
    FileContext spyFileContext = Mockito.spy(FileContext.getLocalFSFileContext());
    IOException mkdirFailure = new IOException("mkdir failed");
    Mockito.doThrow(mkdirFailure)
        .when(spyFileContext)
        .mkdir(Mockito.any(), Mockito.any(), Mockito.anyBoolean());
    Method createDirectory = MasterServiceMain.class.getDeclaredMethod(
        "createDirectory", FileContext.class, String.class);
    createDirectory.setAccessible(true);
    File targetDir = new File(tempFolder.newFolder("mkdirParent"), "twill");
    try {
      createDirectory.invoke(master, spyFileContext, targetDir.getAbsolutePath());
      Assert.fail("Expected RuntimeException wrapping IOException");
    } catch (InvocationTargetException ite) {
      Assert.assertTrue(ite.getCause() instanceof RuntimeException);
      Assert.assertSame(mkdirFailure, ite.getCause().getCause());
    }

    // 2. login(CConfiguration) propagates unchecked and wraps checked IOException
    Method login = MasterServiceMain.class.getDeclaredMethod("login", CConfiguration.class);
    login.setAccessible(true);
    javax.security.auth.login.Configuration prevJaas = getJaasConfigurationQuietly();
    String prevRealm = System.getProperty("java.security.krb5.realm");
    String prevKdc = System.getProperty("java.security.krb5.kdc");
    System.setProperty("java.security.krb5.realm", "EXAMPLE.COM");
    System.setProperty("java.security.krb5.kdc", "localhost");
    Configuration krbHadoopConf = new Configuration(false);
    krbHadoopConf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    try {
      UserGroupInformation.setConfiguration(krbHadoopConf);

      CConfiguration missingPrincipalConf = CConfiguration.create();
      missingPrincipalConf.setBoolean(Constants.Security.ENABLED, true);
      missingPrincipalConf.setBoolean(Constants.Security.KERBEROS_ENABLED, true);
      try {
        login.invoke(master, missingPrincipalConf);
        Assert.fail("Expected IllegalArgumentException");
      } catch (InvocationTargetException ite) {
        Assert.assertTrue(ite.getCause() instanceof IllegalArgumentException);
      }

      File emptyKeytab = tempFolder.newFile("empty.keytab");
      CConfiguration invalidKeytabConf = CConfiguration.create();
      invalidKeytabConf.setBoolean(Constants.Security.ENABLED, true);
      invalidKeytabConf.setBoolean(Constants.Security.KERBEROS_ENABLED, true);
      invalidKeytabConf.set(
          Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL, "cdap/localhost@EXAMPLE.COM");
      invalidKeytabConf.set(
          Constants.Security.CFG_CDAP_MASTER_KRB_KEYTAB_PATH, emptyKeytab.getAbsolutePath());
      try {
        login.invoke(master, invalidKeytabConf);
        Assert.fail("Expected RuntimeException wrapping IOException");
      } catch (InvocationTargetException ite) {
        Assert.assertTrue(ite.getCause() instanceof RuntimeException);
        Assert.assertTrue(ite.getCause().getCause() instanceof IOException);
      }
    } finally {
      UserGroupInformation.setConfiguration(new Configuration());
      javax.security.auth.login.Configuration.setConfiguration(prevJaas);
      restoreProperty("java.security.krb5.realm", prevRealm);
      restoreProperty("java.security.krb5.kdc", prevKdc);
      System.clearProperty(Constants.External.Zookeeper.ENV_AUTH_PROVIDER_1);
      System.clearProperty(Constants.External.Zookeeper.ENV_ALLOW_SASL_FAILED_CLIENTS);
      System.clearProperty("zookeeper.sasl.clientconfig");
    }

    // 3. MasterLeaderElectionHandler.stop(boolean) closes resources quietly when close() throws
    final Object handler = instantiateLeaderElectionHandler(master, cConf);
    MetadataStorage metadataStorage = Mockito.mock(MetadataStorage.class);
    Mockito.doAnswer(inv -> sneakyThrow(new IOException("meta close fail")))
        .when(metadataStorage).close();
    AccessControllerInstantiator accessController =
        Mockito.mock(AccessControllerInstantiator.class);
    Mockito.doAnswer(inv -> sneakyThrow(new IOException("auth close fail")))
        .when(accessController).close();
    LogAppenderInitializer logInitializer = Mockito.mock(LogAppenderInitializer.class);
    Mockito.doAnswer(inv -> sneakyThrow(new IOException("log close fail")))
        .when(logInitializer).close();

    setField(handler, "metadataStorage", metadataStorage);
    setField(handler, "accessControllerInstantiator", accessController);
    setField(handler, "logAppenderInitializer", logInitializer);

    @SuppressWarnings("unchecked")
    List<Service> services = (List<Service>) getField(handler, "services");
    TestIdleService runningSvc = new TestIdleService(false);
    runningSvc.startAsync().awaitRunning();
    services.add(runningSvc);

    Method handlerStop = handler.getClass().getDeclaredMethod("stop", boolean.class);
    handlerStop.setAccessible(true);
    handlerStop.invoke(handler, true);

    Assert.assertEquals(Service.State.TERMINATED, runningSvc.state());
    Assert.assertTrue(services.isEmpty());
    Mockito.verify(metadataStorage).close();
    Mockito.verify(accessController).close();
    Mockito.verify(logInitializer).close();

    // 4. startTwillApplication propagates IOException when local data dir is a regular file
    File regularFile = tempFolder.newFile("notADirectory");
    CConfiguration badDirConf = CConfiguration.create();
    badDirConf.set(Constants.CFG_LOCAL_DATA_DIR, regularFile.getAbsolutePath());
    Object badDirHandler = instantiateLeaderElectionHandler(master, badDirConf);
    Method startTwillApp = null;
    for (Method m : badDirHandler.getClass().getDeclaredMethods()) {
      if ("startTwillApplication".equals(m.getName())) {
        startTwillApp = m;
        break;
      }
    }
    Assert.assertNotNull(startTwillApp);
    startTwillApp.setAccessible(true);
    try {
      startTwillApp.invoke(
          badDirHandler, Mockito.mock(TwillRunnerService.class), null, null);
      Assert.fail("Expected RuntimeException wrapping IOException");
    } catch (InvocationTargetException ite) {
      Assert.assertTrue(ite.getCause() instanceof RuntimeException);
      Assert.assertTrue(ite.getCause().getCause() instanceof IOException);
    }
  }

  private MasterServiceMain createMasterWithConf(CConfiguration siteConf) throws Exception {
    final MasterServiceMain[] holder = new MasterServiceMain[1];
    runWithSiteConf(siteConf, () -> holder[0] = new MasterServiceMain());
    return holder[0];
  }

  private Object instantiateLeaderElectionHandler(MasterServiceMain master, CConfiguration cConf)
      throws Exception {
    Class<?> handlerClass = null;
    for (Class<?> inner : MasterServiceMain.class.getDeclaredClasses()) {
      if ("MasterLeaderElectionHandler".equals(inner.getSimpleName())) {
        handlerClass = inner;
        break;
      }
    }
    Assert.assertNotNull(handlerClass);
    Constructor<?> ctor = handlerClass.getDeclaredConstructor(
        MasterServiceMain.class,
        CConfiguration.class,
        Configuration.class,
        ZKClientService.class,
        LeaderElectionInfoService.class);
    ctor.setAccessible(true);
    ZKClientService zk = Mockito.mock(ZKClientService.class);
    return ctor.newInstance(
        master, cConf, new Configuration(), zk, Mockito.mock(LeaderElectionInfoService.class));
  }

  @SuppressWarnings("unchecked")
  private static <T, E extends Throwable> T sneakyThrow(Throwable t) throws E {
    throw (E) t;
  }

  private static void restoreProperty(String key, String value) {
    if (value == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, value);
    }
  }

  private static javax.security.auth.login.Configuration getJaasConfigurationQuietly() {
    try {
      return javax.security.auth.login.Configuration.getConfiguration();
    } catch (SecurityException e) {
      return null;
    }
  }

  private static void setField(Object target, String fieldName, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static Object getField(Object target, String fieldName) throws Exception {
    Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    return field.get(target);
  }

  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  private void runWithSiteConf(CConfiguration siteConf, ThrowingRunnable action) throws Exception {
    File siteFile = new File(tempFolder.newFolder(), "cdap-site.xml");
    try (FileOutputStream out = new FileOutputStream(siteFile)) {
      siteConf.writeXml(out);
    }
    URL fileUrl = siteFile.toURI().toURL();

    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    ClassLoader customLoader = new ClassLoader(previous) {
      @Override
      public URL getResource(String name) {
        if ("cdap-site.xml".equals(name)) {
          return fileUrl;
        }
        return super.getResource(name);
      }

      @Override
      public Enumeration<URL> getResources(String name) throws IOException {
        if ("cdap-site.xml".equals(name)) {
          return Collections.enumeration(Collections.singletonList(fileUrl));
        }
        return super.getResources(name);
      }
    };

    Thread.currentThread().setContextClassLoader(customLoader);
    try {
      action.run();
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
  }

  private static final class TestIdleService extends AbstractIdleService {
    private final boolean failOnStop;

    TestIdleService(boolean failOnStop) {
      this.failOnStop = failOnStop;
    }

    @Override
    protected void startUp() {
    }

    @Override
    protected void shutDown() throws Exception {
      if (failOnStop) {
        throw new IOException("simulated stop failure");
      }
    }
  }
}
