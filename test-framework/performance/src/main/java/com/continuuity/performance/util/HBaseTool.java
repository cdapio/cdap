package com.continuuity.performance.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * HBase client tool that helps with preparing and analyzing benchmark runs.
 */
public final class HBaseTool {
  private Configuration hbConfig;
  private HBaseAdmin hba;
  private String command;
  private boolean prompt = true;

  private static final Logger LOG = LoggerFactory.getLogger(HBaseTool.class);

  /**
   * Prints the usage information.
   */
  private void usage() {
    System.out.println("Usage:");
    System.out.println("  hbase-tool --dropAllTables --noPrompt --zkHost <host> [ --zkPort <port> ]");
    System.out.println("  hbase-tool --listNodes --noPrompt --zkHost <host> [ --zkPort <port> ]");
    System.out.println("  hbase-tool --help");
  }

  private boolean parseArgs(String[] args) {
    hbConfig = HBaseConfiguration.create();
    boolean help = false;

    for (int i = 0; i < args.length; i++) {
      if ("--help".equals(args[i])) {
        help = true;
        continue;
      } else if ("--dropAllTables".equals(args[i])) {
        command = "dropAllTables";
        continue;
      } else if ("--noPrompt".equals(args[i])) {
        prompt = false;
        continue;
      } else if (args[i].startsWith("--")) {
        if (i + 1 < args.length) {
          String key = args[i].substring(2);
          String value = args[ ++i ];
          if ("zkHost".equals(key)) {
            hbConfig.set("hbase.zookeeper.quorum", value);
          } else if ("zkPort".equals(key)) {
            hbConfig.set("hbase.zookeeper.property.clientPort", value);
          }
        }
      }
    }

    if (help
      || prompt
      || command == null || command.isEmpty()
      || hbConfig.get("hbase.zookeeper.quorum") == null
      || hbConfig.get("hbase.zookeeper.property.clientPort") == null) {
      usage();
      return false;
    }
    return true;
  }

  private void execute() throws IOException {
    hba = new HBaseAdmin(hbConfig);
    if ("dropAllTables".equals(command)) {
      if (!prompt) {
        dropAllTables();
      }
    }
  }

  private void dropAllTables() throws IOException {
    for (HTableDescriptor hTableDescriptor : hba.disableTables(Pattern.compile(".*"))) {
      LOG.info("Disabled table " + hTableDescriptor);
    }
    for (HTableDescriptor hTableDescriptor : hba.deleteTables(Pattern.compile(".*"))) {
      LOG.info("Deleted table " + hTableDescriptor);
    }
  }

  public static void main(String[] args) throws IOException {
    HBaseTool hbt = new HBaseTool();
    boolean ok = hbt.parseArgs(args);
    if (ok) {
      hbt.execute();
    }
    if (!ok) {
      System.exit(1);
    }
  }
}
