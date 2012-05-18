package com.continuuity.common.conflake;

import com.continuuity.common.zookeeper.InMemoryZookeeper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class ConflakeServiceTest {

  @Test
  public void testConflakeService() throws Exception {
    List<Closeable> closeables = Lists.newArrayList();
    InMemoryZookeeper server = new InMemoryZookeeper();
    closeables.add(server);

    try {
      String zkEnsemble = server.getConnectionString();
      boolean status = ConflakeService.start(zkEnsemble);
      Assert.assertTrue(status);
      ImmutableList<Long> ids = ConflakeService.getIds(1);
      Assert.assertTrue(ids.size() == 1);
      ids = ConflakeService.getIds(2);
      Assert.assertTrue(ids.size() == 2);
      ids = ConflakeService.getIds(0);
      Assert.assertNotNull(ids);
      ids = ConflakeService.getIds(10);
      Assert.assertTrue(ids.size() == 10);
    } finally {
      ConflakeService.stop();
      Collections.reverse(closeables);
      for(Closeable c : closeables) {
        Closeables.closeQuietly(c);
      }
    }

  }

}
