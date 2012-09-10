package com.continuuity.common.zookeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

/**
 * InMemoryZookeeper provides an in-memory implementation of ZooKeeper.
 *
 * Following is an example of how InMemoryZookeeper can be used:
 * <code>
 *   Closeable closeable = new Closeable();
 *   InMemoryZookeeper zk = new InMemoryZookeeper();
 *   closeable = zk;
 *   ...
 *   ...
 *   String connectionString = zk.getConnectionString();
 *   ...
 *   // Now, this connection string can be passed to client connecting to this
 *   // instance of ZK.
 *   ...
 *   Closeables.closeQuietly(closeable);
 * </code>
 */
public class InMemoryZookeeper implements Closeable {
  private final static Logger LOG = LoggerFactory.getLogger(InMemoryZookeeper.class);
  private final InMemoryZookeeperServer inMemoryZookeeperServer;
  private final InstanceSpecification specification;

  /**
   * Constructor that starts on available port and temporary directory path.
   * @throws Exception
   */
  public InMemoryZookeeper() throws InterruptedException, IOException {
    this(-1, null);
  }

  /**
   * Constructor to start Zookeeper on particular port.
   * @param port on which ZK need to be started.
   * @throws Exception
   */
  public InMemoryZookeeper(int port) throws InterruptedException, IOException {
    this(port, null);
  }

  /**
   * Constructor to start Zookeeper on particular port and ZK directory path.
   * @param port on which ZK need to be started.
   * @param temporaryDir under which all data gets stored.
   * @throws Exception
   */
  public InMemoryZookeeper(int port, File temporaryDir) throws InterruptedException, IOException {
    this(new InstanceSpecification(temporaryDir, port, -1, -1, true, -1));
  }

  /**
   * Constructor to start the Zookeeper on default port and ZK temporary directory path.
   *
   * @param temporaryDir under which all data gets stored.
   * @throws InterruptedException
   * @throws IOException
   */
  public InMemoryZookeeper(File temporaryDir) throws InterruptedException, IOException {
    this(new InstanceSpecification(temporaryDir, 2181, -1, -1, true, -1));
  }

  /**
   * Constructor with instance specification.
   * @param specification
   * @throws Exception
   */
  private InMemoryZookeeper(InstanceSpecification specification) throws InterruptedException, IOException {
    this.specification = specification;
    inMemoryZookeeperServer = new InMemoryZookeeperServer(new QuorumConfigBuilder(specification));
    // Disables event logging and connection issues.
    System.setProperty("curator-log-events", "false");
    System.setProperty("curator-dont-log-connection-problems", "true");
    LOG.info("Starting InMemoryZookeeper ...");
    inMemoryZookeeperServer.start();
  }

  /**
   * @return Returns the connection string for ZK.
   */
  public String getConnectionString() {
    return specification.getConnectString();
  }

  /**
   * @return Returns temporary directory under which this ZK will run.
   */
  public File getTemporaryDirectory() {
    return specification.getDataDirectory();
  }

  /**
   * @return Port under which ZK is running.
   */
  public int getPort() {
    return specification.getPort();
  }

  /**
   * @return Server ID of the ZK instance running.
   */
  public int getServerId() {
    return specification.getServerId();
  }

  /**
   * Stops a running instance of ZK.
   * @throws IOException
   */
  public void stop() throws IOException {
    LOG.info("Stopping InMemoryZookeeper ...");
    inMemoryZookeeperServer.stop();
  }

  /**
   * Closes connection with ZK.
   * @throws IOException
   */
  public void close() throws IOException {
    inMemoryZookeeperServer.close();
  }

}
