package com.continuuity.hive.client;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;

import com.google.inject.Inject;
import org.apache.hive.beeline.BeeLine;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

/**
 * Executes commands on Hive using beeline on a discovered HiveServer2.
 */
public class HiveCommandExecutor implements HiveClient {
  private static final Logger LOG = LoggerFactory.getLogger(HiveCommandExecutor.class);

  public final DiscoveryServiceClient discoveryClient;

  @Inject
  public HiveCommandExecutor(DiscoveryServiceClient discoveryClient) {
    this.discoveryClient = discoveryClient;
  }

  @Override
  public void sendCommand(String cmd, OutputStream out, OutputStream err) throws IOException {

    Discoverable hiveDiscoverable = new TimeLimitEndpointStrategy(
        new RandomEndpointStrategy(discoveryClient.discover(Constants.Service.HIVE)), 3L, TimeUnit.SECONDS).pick();
    if (hiveDiscoverable == null) {
      LOG.error("No endpoint for service {}", Constants.Service.HIVE);
      throw new IOException("No endpoint for service " + Constants.Service.HIVE);
    }

    // The hooks are plugged in for every command
    String[] args = new String[] {"-d", BeeLine.BEELINE_DEFAULT_JDBC_DRIVER,
        "-u", BeeLine.BEELINE_DEFAULT_JDBC_URL +
        hiveDiscoverable.getSocketAddress().getHostName() +
        ":" + hiveDiscoverable.getSocketAddress().getPort() +
        "/default;auth=noSasl?" +
        "hive.exec.pre.hooks=com.continuuity.hive.hooks.TransactionPreHook;" +
        "hive.exec.post.hooks=com.continuuity.hive.hooks.TransactionPostHook",
        "-n", "hive",
        "--outputformat=table",
        "-e", cmd};

    BeeLine beeLine = new BeeLine();
    if (out != null) {
      PrintStream pout = new PrintStream(out);
      beeLine.setOutputStream(pout);
    }
    if (err != null) {
      PrintStream perr = new PrintStream(err);
      beeLine.setErrorStream(perr);
    }
    beeLine.begin(args, null);
    beeLine.close();
  }
}
