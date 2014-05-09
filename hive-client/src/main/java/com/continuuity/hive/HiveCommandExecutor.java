package com.continuuity.hive;

import com.continuuity.common.hive.HiveClient;
import com.google.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import org.apache.hive.beeline.BeeLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class HiveCommandExecutor implements HiveClient {
  private static final Logger LOG = LoggerFactory.getLogger(HiveCommandExecutor.class);

  HiveServer hiveServer;

  @Inject
  public HiveCommandExecutor(HiveServer hiveServer) {
    // todo startAndWait and stopAndWait in singleNodeMain or whatever that is
    this.hiveServer = hiveServer;
  }

  @Override
  public void sendCommand(String cmd) throws IOException {
    String[] args = new String[] {"-d", BeeLine.BEELINE_DEFAULT_JDBC_DRIVER,
        "-u", BeeLine.BEELINE_DEFAULT_JDBC_URL + "localhost:" + hiveServer.getHiveServerPort() +
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
    LOG.info(out.toString("UTF-8"));
  }
}
