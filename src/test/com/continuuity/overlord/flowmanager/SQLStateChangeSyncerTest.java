package com.continuuity.overlord.flowmanager;

import com.continuuity.common.zookeeper.InMemoryZookeeper;
import com.continuuity.flowmanager.SQLStateChangeSyncer;
import com.continuuity.flowmanager.StateChangeListener;
import com.continuuity.flowmanager.StateChangeType;
import com.continuuity.flowmanager.StateChanger;
import com.continuuity.flowmanager.internal.StateChange;
import com.google.common.io.Closeables;
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
    org.hsqldb.util.DatabaseManagerSwing.main(new String[] {
      "--url",  "jdbc:hsqldb:mem:testdb", "--noexit"
    });
  }

  @AfterClass
  public static void after() throws Exception {
    if(zookeeper != null) {
      Closeables.closeQuietly(zookeeper);
    }
  }

  @Test
  public void testHSQLStateChangeSyncerSave() throws Exception {
    CuratorFramework client = CuratorFrameworkFactory.newClient(zkEnsemble, new RetryOneTime(10)); client.start();


    StateChanger changer = StateChange.Client.newState(client, "/continuuity/system/queue");
    StateChangeListener listener = StateChange.Server.newListener(client);

    SQLStateChangeSyncer stateChangeSyncer = new SQLStateChangeSyncer("jdbc:hsqldb:mem:testdb");
    listener.listen("/continuuity/system/queue", stateChangeSyncer);

    for(int i = 0; i < 100; ++i) {
      changer.change(StateChange.Client.newState("A:" + i, "B", "C", "[]",
        StateChangeType.DEPLOYED_FLOW));
    }

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {

    }

    Connection connection = stateChangeSyncer.getConnection();
    String sql = "SELECT COUNT(*)  FROM flow_state;";
    PreparedStatement stmt = connection.prepareStatement(sql);
    ResultSet rs = stmt.executeQuery();

    Assert.assertTrue(rs.getFetchSize() == 1);
    rs.next();
    int i = rs.getInt(1);
    Assert.assertTrue(rs.getInt(1) == 100);
    connection.close();
    Closeables.closeQuietly(stateChangeSyncer);
    Closeables.closeQuietly(listener);
  }
}
