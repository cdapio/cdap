package com.continuuity.hive;

import junit.framework.Assert;
import org.apache.hive.beeline.BeeLine;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.net.URL;

/**
 *
 */
public class TestHive {

  @ClassRule
  public static final EmbeddedHiveServer embeddedHiveServer = new EmbeddedHiveServer();

  private String runHiveCommand(String cmd) throws Exception {
    String[] args = new String[] {"-d", BeeLine.BEELINE_DEFAULT_JDBC_DRIVER,
      "-u", BeeLine.BEELINE_DEFAULT_JDBC_URL + "localhost:" + embeddedHiveServer.getHiveServerPort() +
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

    System.out.println("********* HIVE OUT = [" + out.toString("UTF-8") + "]");
    System.out.println("********* HIVE ERR = [" + err.toString("UTF-8") + "]");
    return out.toString("UTF-8");
  }

  @Test
  public void testHive() throws Exception {
    URL loadFileUrl = getClass().getResource("/test_table.dat");
    Assert.assertNotNull(loadFileUrl);

    runHiveCommand("drop table if exists test;");
    runHiveCommand("create table test (first INT, second STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t';");
    runHiveCommand("show tables;");
    runHiveCommand("describe test;");
    runHiveCommand("LOAD DATA LOCAL INPATH '" + new File(loadFileUrl.toURI()).getAbsolutePath() + "' INTO TABLE test;");
    runHiveCommand("select first, second from test;");
    runHiveCommand("drop table test;");
  }

//  @Test
//  public void testExternalTable() throws Exception {
//    runHiveCommand("drop table if exists employee");
//    runHiveCommand("create external table employee (name STRING, id int) " +
//                     "stored by 'com.continuuity.hive.datasets.DatasetStorageHandler';");
//    runHiveCommand("show tables;");
//    runHiveCommand("describe employee;");
//    runHiveCommand("select name, id from employee;");
//    runHiveCommand("select * from employee;");
//    runHiveCommand("drop table employee");
//  }
}
