package com.continuuity.performance.util;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Zookeeper client tool that deletes Reactor related z-nodes from Zookeeper.
 */
public final class ZookeeperTool {
  private static final int SESSION_TIMEOUT = 30000; // 30 seconds
  private static final String[] REACTOR_NODES = {"/continuuity_kafka", "/discoverable", "/LogSaverWeaveApplication"};

  private String zkHost;
  private int zkPort = 2181;
  private String command;
  private boolean prompt = true;

  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperTool.class);

  private static ZooKeeper connect(String hosts) throws IOException, InterruptedException {
    final CountDownLatch signal = new CountDownLatch(1);
    ZooKeeper zk = new ZooKeeper(hosts, SESSION_TIMEOUT, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
          signal.countDown();
        }
      }
    });
    signal.await();
    return zk;
  }

  /**
   * Prints the usage information.
   */
  private void usage() {
    System.out.println("Usage:");
    System.out.println("  zk-tool --dropReactorNodes --noPrompt --zkHost <host> [ --zkPort <port> ]");
    System.out.println("  zk-tool --listNodes --noPrompt --zkHost <host> [ --zkPort <port> ]");
    System.out.println("  zk-tool --help");
  }

  private boolean parseArgs(String[] args) {
    boolean help = false;

    for (int i = 0; i < args.length; i++) {
      if ("--help".equals(args[i])) {
        help = true;
        continue;
      } else if ("--dropReactorNodes".equals(args[i])) {
        command = "dropReactorNodes";
        continue;
      } else if ("--listNodes".equals(args[i])) {
        command = "listNodes";
        continue;
      } else if ("--noPrompt".equals(args[i])) {
        prompt = false;
        continue;
      } else if (args[i].startsWith("--")) {
        if (i + 1 < args.length) {
          String key = args[i].substring(2);
          String value = args[ ++i ];
          if ("zkHost".equals(key)) {
            zkHost = value;
          } else if ("zkPort".equals(key)) {
            zkPort = Integer.valueOf(value);
          }
        }
      }
    }
    if (help
      || prompt
      || command == null || command.isEmpty()
      || zkHost == null) {
      usage();
      return false;
    }
    return true;
  }

  private void execute() throws IOException, InterruptedException, KeeperException {
    if ("dropReactorNodes".equals(command)) {
      if (!prompt) {
        dropReactorNodes(connect(zkHost + ":" + zkPort));
      }
    } else if ("listNodes".equals(command)) {
      if (!prompt) {
        listNodes(connect(zkHost + ":" + zkPort));
      }
    }
  }

  private void dropReactorNodes(ZooKeeper zooKeeper) throws IOException, KeeperException, InterruptedException {
    for (String deleteNode : REACTOR_NODES) {
      if (zooKeeper.exists(deleteNode, false) != null) {
        LOG.info("Deleting top level znode {}.", deleteNode);
        ZKUtil.deleteRecursive(zooKeeper, deleteNode);
      }
    }
  }

  private void listNodes(ZooKeeper zooKeeper) throws KeeperException, InterruptedException {
    LOG.info("List of top level znodes in Zookeeper on {}", zkHost + ":" + zkPort);
    for (String topLevelNode : zooKeeper.getChildren("/", false)) {
      LOG.info("/{}", topLevelNode);
    }
  }

  public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
    ZookeeperTool zkt = new ZookeeperTool();
    boolean ok = zkt.parseArgs(args);
    if (ok) {
      zkt.execute();
    }
    if (!ok) {
      System.exit(1);
    }
  }
}
