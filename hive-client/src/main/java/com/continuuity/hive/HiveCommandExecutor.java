package com.continuuity.hive;

import com.google.inject.Inject;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.hive.HiveClient;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;
import org.apache.hive.beeline.BeeLine;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class HiveCommandExecutor implements HiveClient {
  private static final Logger LOG = LoggerFactory.getLogger(HiveCommandExecutor.class);

  public final DiscoveryServiceClient discoveryClient;

  @Inject
  public HiveCommandExecutor(DiscoveryServiceClient discoveryClient) {
    this.discoveryClient = discoveryClient;
  }

  @Override
  public void sendCommand(String cmd) throws IOException {

    Iterable<Discoverable> hiveDiscoverables = discoveryClient.discover(Constants.Service.HIVE);
    Iterator<Discoverable> iterator = hiveDiscoverables.iterator();
    if (!iterator.hasNext()) {
      // todo throw some exception I guess
      return;
    }
    // There should only be one hive discoverable
    Discoverable hiveDiscoverable = iterator.next();

    String[] args = new String[] {"-d", BeeLine.BEELINE_DEFAULT_JDBC_DRIVER,
        "-u", BeeLine.BEELINE_DEFAULT_JDBC_URL +
        hiveDiscoverable.getSocketAddress().getHostName() +
        ":" + hiveDiscoverable.getSocketAddress().getPort() +
        "/default;auth=noSasl",
        "-n", "poorna",
        "-e", cmd};

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream pout = new PrintStream(out);

    ByteArrayOutputStream err = new ByteArrayOutputStream();
    PrintStream perr = new PrintStream(err);

    BeeLine beeLine = new BeeLine();
    beeLine.setOutputStream(pout);
    beeLine.setErrorStream(perr);
    beeLine.begin(args, null);
    beeLine.close();

    LOG.info("********* HIVE OUT = [" + out.toString("UTF-8") + "]");
    LOG.info("********* HIVE ERR = [" + err.toString("UTF-8") + "]");
  }
}
