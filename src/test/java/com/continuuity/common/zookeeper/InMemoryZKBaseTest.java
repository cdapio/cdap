package com.continuuity.common.zookeeper;

import com.continuuity.common.zookeeper.InMemoryZookeeper;
import org.junit.After;
import org.junit.Before;

/**
 *
 */
public class InMemoryZKBaseTest {
  protected InMemoryZookeeper server;

  @Before
  public void setupServer() throws Exception {
    server = new InMemoryZookeeper();
  }

  @After
  public void tearDownServer() throws Exception {
    server.close();
  }
}

