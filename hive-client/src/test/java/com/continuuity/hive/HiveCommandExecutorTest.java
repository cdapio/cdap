package com.continuuity.hive;

import junit.framework.Assert;
import org.apache.hive.beeline.BeeLine;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.net.URL;

/**
 *
 */
  public class HiveCommandExecutorTest {
  private static final Logger LOG = LoggerFactory.getLogger(HiveCommandExecutorTest.class);

  @Test
  public void testHive() throws Exception {
    URL loadFileUrl = getClass().getResource("/test_table.dat");
    Assert.assertNotNull(loadFileUrl);

    sendCommand("drop table if exists test;");
    sendCommand("create table test (first INT, second STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t';");
    sendCommand("show tables;");
    sendCommand("describe test;");
    sendCommand("LOAD DATA LOCAL INPATH '" + new File(loadFileUrl.toURI()).getAbsolutePath() + "' INTO TABLE test;");
    sendCommand("select first, second from test;");
    sendCommand("drop table test;");
  }

  @Test
  public void testHive2() throws Exception {
    sendCommand("drop table if exists purchase");
    sendCommand("create external table purchase (customer STRING, quantity int) " +
                  "stored by 'com.continuuity.hive.datasets.DatasetStorageHandler' " +
                  "with serdeproperties (\"reactor.dataset.name\"=\"purchases\") ;");
    sendCommand("show tables;");
    sendCommand("describe purchase;");
    sendCommand("select customer, quantity from purchase;");
    sendCommand("select * from purchase;");
    sendCommand("select customer, quantity from purchase where customer = \"Tom\";");
    sendCommand("select * from purchase where customer = \"George\";");
    sendCommand("drop table purchase");
  }

  private void sendCommand(String cmd) throws Exception {
    String[] args = new String[] {"-d", BeeLine.BEELINE_DEFAULT_JDBC_DRIVER,
      "-u", BeeLine.BEELINE_DEFAULT_JDBC_URL +
      "localhost" +
      ":" + "54216" +
      "/default;auth=noSasl",
      "-n", "poorna",
      "-e", cmd};

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream pout = new PrintStream(out);

    ByteArrayOutputStream err = new ByteArrayOutputStream();
    PrintStream perr = new PrintStream(err);

    LOG.info("********* Command = " + cmd);
    BeeLine beeLine = new BeeLine();
    beeLine.setOutputStream(pout);
    beeLine.setErrorStream(perr);
    beeLine.begin(args, null);
    beeLine.close();

    LOG.info("********* HIVE OUT = [" + out.toString("UTF-8") + "]");
    LOG.info("********* HIVE ERR = [" + err.toString("UTF-8") + "]");
  }
}
