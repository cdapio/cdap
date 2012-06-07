package com.continuuity.observer;

import com.continuuity.common.zookeeper.InMemoryZookeeper;
import com.continuuity.observer.internal.StateChange;
import com.continuuity.runtime.DIModules;
import com.google.common.io.Closeables;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryOneTime;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 *
 *
 */
public class SQLStateChangeSyncerTest {

  private static final Logger Log = LoggerFactory.getLogger(SQLStateChangeSyncerTest.class);
  private static InMemoryZookeeper zookeeper = null;
  private static String zkEnsemble;


  @BeforeClass
  public static void before() throws Exception {
    zookeeper = new InMemoryZookeeper();
    zkEnsemble = zookeeper.getConnectionString();
    Log.info("Connection string {}", zkEnsemble);
    try {
      Class.forName("org.hsqldb.jdbcDriver");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  @AfterClass
  public static void after() throws Exception {
    if (zookeeper != null) {
      Closeables.closeQuietly(zookeeper);
    }
  }

  @Test
  public void testHSQLStateChangeSyncerSave() throws Exception {
    CuratorFramework client = CuratorFrameworkFactory.newClient(zkEnsemble, new RetryOneTime(10));
    client.start();


    StateChanger changer = StateChange.Client.newStateChanger(client, "/continuuity/system/queue");
    StateChangeListener listener = StateChange.Server.newListener(client);

    Injector injector = Guice.createInjector(DIModules.getInMemoryHSQLBindings());
    StateChangeCallback callback = injector.getInstance(StateChangeCallback.class);
    listener.listen("/continuuity/system/queue", callback);

    for (int i = 0; i < 100; ++i) {
      changer.change(StateChange.Client.newState("A:" + i, "B", "C", "[]",
        StateChangeType.DEPLOYED));
    }

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {

    }

    Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:fmdb");
    String sql = "SELECT COUNT(*)  FROM flow_state;";
    PreparedStatement stmt = connection.prepareStatement(sql);
    ResultSet rs = stmt.executeQuery();

    Assert.assertTrue(rs.getFetchSize() == 1);
    rs.next();
    int i = rs.getInt(1);
    Assert.assertTrue(rs.getInt(1) == 100);
    connection.close();
    Closeables.closeQuietly(callback);
    Closeables.closeQuietly(listener);
  }
}
