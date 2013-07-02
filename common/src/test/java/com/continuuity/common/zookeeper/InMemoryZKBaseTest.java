package com.continuuity.common.zookeeper;

import com.continuuity.common.utils.OSDetector;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

/**
 *
 */
public class InMemoryZKBaseTest {
  protected InMemoryZookeeper server;

  @Before
  public void setupServer() throws Exception {
    server = new InMemoryZookeeper();
    System.out.println("Server started on " + server.getConnectionString());
  }

  public String getEnsemble() {
    return server.getConnectionString();
  }

  @After
  public void tearDownServer() throws Exception {
    try {
      server.close();
    } catch (IOException ioe) {
      // Windows fails here so going to ignore
      if (ioe.getMessage().startsWith("Failed to delete") && OSDetector.isWindows()) {
        // do nothing
      } else {
        throw ioe;
      }
    }
  }
}

