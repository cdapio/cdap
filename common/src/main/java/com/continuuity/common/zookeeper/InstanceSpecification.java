package com.continuuity.common.zookeeper;

import com.google.common.base.Objects;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstracts one of the servers in the ensemble
 */
class InstanceSpecification {
  private static final Logger LOG = LoggerFactory.getLogger(InstanceSpecification.class);
  private static final AtomicInteger nextServerId = new AtomicInteger(1);

  private final File      dataDirectory;
  private final int       port;
  private final int       electionPort;
  private final int       quorumPort;
  private final boolean   deleteDataDirectoryOnClose;
  private final int serverId;

  public static InstanceSpecification newInstanceSpec() {
    return new InstanceSpecification(null, -1, -1, -1, true, -1);
  }

  /**
   * Next port that available in the system.
   * @return the next port that's available in the system.
   */
  public static int getRandomPort() {
    ServerSocket server = null;
    try {
      server = new ServerSocket(0);
      return server.getLocalPort();
    } catch (IOException e) {
      LOG.error("Failed to find a local port " + e.getMessage());
    } finally {
      if (server != null) {
        try {
          server.close();
        } catch (IOException ignore) {}
      }
    }
    return -1;
  }

  /**
   * @param dataDirectory where to store data/logs/etc.
   * @param port the port to listen on - each server in the ensemble must use a unique port
   * @param electionPort the electionPort to listen on - each server in the ensemble must use a unique electionPort
   * @param quorumPort the quorumPort to listen on - each server in the ensemble must use a unique quorumPort
   * @param deleteDataDirectoryOnClose if true, the data directory will be deleted when
   *                                   {@link InMemoryZookeeper#close()} is called
   * @param serverId the server ID for the instance
   */
  public InstanceSpecification(File dataDirectory, int port, int electionPort,
                               int quorumPort, boolean deleteDataDirectoryOnClose, int serverId) {
    this.dataDirectory = (dataDirectory != null) ? dataDirectory : Files.createTempDir();
    this.port = (port >= 0) ? port : getRandomPort();
    this.electionPort = (electionPort >= 0) ? electionPort : getRandomPort();
    this.quorumPort = (quorumPort >= 0) ? quorumPort : getRandomPort();
    this.deleteDataDirectoryOnClose = deleteDataDirectoryOnClose;
    this.serverId = (serverId >= 0) ? serverId : nextServerId.getAndIncrement();
  }

  public int getServerId() {
    return serverId;
  }

  public File getDataDirectory() {
    return dataDirectory;
  }

  public int getPort() {
    return port;
  }

  public int getElectionPort() {
    return electionPort;
  }

  public int getQuorumPort() {
    return quorumPort;
  }

  public String getConnectString() {
    return "localhost:" + port;
  }

  public boolean deleteDataDirectoryOnClose() {
    return deleteDataDirectoryOnClose;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
            .add("dataDirectory", dataDirectory)
            .add("port", port)
            .add("electionPort", electionPort)
            .add("quorumPort", quorumPort)
            .add("deleteDataDirectoryOnClose", deleteDataDirectoryOnClose)
            .add("serverId", serverId)
            .toString();
  }

  @Override
  public boolean equals(Object o) {
    InstanceSpecification other = (InstanceSpecification) o;
    return Objects.equal(port, other.port);
  }

  @Override
  public int hashCode(){
    return Objects.hashCode(port);
  }
}
