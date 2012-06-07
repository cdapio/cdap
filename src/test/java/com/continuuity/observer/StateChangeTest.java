package com.continuuity.observer;

import com.continuuity.common.zookeeper.InMemoryZookeeper;
import com.continuuity.observer.internal.StateChange;
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

import java.io.IOException;

/**
 *
 */
public class StateChangeTest {
  private static final Logger Log = LoggerFactory.getLogger(StateChangeTest.class);
  private static InMemoryZookeeper zookeeper = null;
  private static String zkEnsemble;


  @BeforeClass
  public static void before() throws Exception {
    zookeeper = new InMemoryZookeeper();
    zkEnsemble = zookeeper.getConnectionString();
    Log.info("Connection string {}", zkEnsemble);
  }

  @AfterClass
  public static void after() throws Exception {
    if (zookeeper != null) {
      Closeables.closeQuietly(zookeeper);
    }
  }

  @Test
  public void testSimpleEventsOrdered() throws Exception {
    CuratorFramework client = CuratorFrameworkFactory.newClient(zkEnsemble, new RetryOneTime(10));
    client.start();
    StateChanger changer = StateChange.Client.newStateChanger(client, "/continuuity/system/queue");
    StateChangeListener listener = StateChange.Server.newListener(client);

    for (int i = 0; i < 100; ++i) {
      changer.change(StateChange.Client.newState("A:" + i, "B", "C", "[]",
        StateChangeType.DEPLOYED));
    }


    listener.listen("/continuuity/system/queue", new StateChangeCallback<StateChangeData>() {
      private int i = 0;

      @Override
      public void process(StateChangeData data) {
        Assert.assertTrue(data.getAccountId().equals("A:" + i));
        Assert.assertTrue(data.getApplication().equals("B"));
        Assert.assertTrue(data.getType() == StateChangeType.DEPLOYED);
        ++i;
        Log.info(data.toString());
      }

      @Override
      public void close() throws IOException {
      }
    });

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {

    }
    Closeables.closeQuietly(listener);

  }

}
