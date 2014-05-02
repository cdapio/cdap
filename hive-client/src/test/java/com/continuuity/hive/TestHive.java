package com.continuuity.hive;

import org.apache.hive.beeline.BeeLine;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

/**
 *
 */
public class TestHive {

  @ClassRule
  public static final EmbeddedHiveServer embeddedHiveServer = new EmbeddedHiveServer();

  private String runHiveCommand(String cmd) throws Exception {
    String[] args = new String[] {"-d", BeeLine.BEELINE_DEFAULT_JDBC_DRIVER,
      "-u", BeeLine.BEELINE_DEFAULT_JDBC_URL + "localhost:" + embeddedHiveServer.getServerPort(), "-n", "root",
      "-e", cmd};

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream pout = new PrintStream(out);
    BeeLine beeLine = new BeeLine();
    beeLine.setOutputStream(pout);
    beeLine.setErrorStream(pout);
    beeLine.begin(args, null);
    beeLine.close();

    System.out.println(out.toString("UTF-8"));
    return out.toString("UTF-8");
  }

  @Test
  public void testHive() throws Exception {
    runHiveCommand("drop table test;");
    runHiveCommand("create table test (first INT, second STRING);");
    runHiveCommand("show tables;");
    runHiveCommand("describe test;");
  }
}
