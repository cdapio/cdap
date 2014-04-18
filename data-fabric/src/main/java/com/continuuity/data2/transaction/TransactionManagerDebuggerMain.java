package com.continuuity.data2.transaction;

import com.continuuity.data2.transaction.inmemory.ChangeId;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.persist.SnapshotCodecV2;
import com.continuuity.data2.transaction.persist.TransactionSnapshot;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.HttpURLConnection;
import java.util.Map;
import java.util.Date;

/**
 *
 */
public class TransactionManagerDebuggerMain {

  private static final SimpleDateFormat formatter
      = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S z");

  private static final Logger LOG =
      LoggerFactory.getLogger(TransactionManagerDebuggerMain.class);

  /**
   *
   * @param args Our cmdline arguments
   */
  public static void main(String[] args) {

    CommandLineParser parser = new GnuParser();
    Options options = new Options();
    options.addOption("h", "hostname", true, "Hostname");
    options.addOption("f", "existingFile", true, "Existing snapshot");

    String hostname;
    String existingFilename;

    // Check all the options of command line
    try {
      CommandLine line = parser.parse(options, args);
      if (!line.hasOption("hostname") && !line.hasOption("existingFile")) {
        LOG.error("Either specify a hostname to retrieve a snapshot from, or an existing snapshot.");
        return;
      }

      hostname = line.getOptionValue("hostname");
      existingFilename = line.getOptionValue("existingFile");
    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("SandboxJVM", options);
      return ;
    }

    TransactionSnapshot snapshot = null;
    if (hostname != null) {
      // Take new snapshot and download it
      snapshot = takeSnapshot(hostname);
    } else if(existingFilename != null) {
      // Retrieve saved snapshot
      snapshot = retrieveSnapshot(existingFilename);
    }

    if (snapshot != null) {
      printSnapshotInfo(snapshot);
    }
  }


  private static TransactionSnapshot retrieveSnapshot(String filename) {
    try {
      LOG.info("Retrieving snapshot from file {}", filename);
      File snapshotFile = new File(filename);
      byte[] encodedSnapshot = Files.toByteArray(snapshotFile);
      SnapshotCodecV2 codec = new SnapshotCodecV2();
      TransactionSnapshot snapshot = codec.decodeState(encodedSnapshot);
      LOG.info("Snapshot retrieved, timestamp is {} ms", snapshot.getTimestamp());
      return snapshot;
    } catch (IOException e) {
      LOG.error("File {} does not exist on disk.", filename);
      return null;
    }
  }

  private static TransactionSnapshot takeSnapshot(String hostname) {
    URL url;
    HttpURLConnection connection = null;
    try {
      url = new URL("http://" + hostname + ":10000/v2/transactions/debug");
      connection = (HttpURLConnection) url.openConnection();

      LOG.info("About to take a snapshot of the transaction manager at {}, timestamp is {} ms", url.toURI(),
          System.currentTimeMillis());
      int responseCode = connection.getResponseCode();
      if (responseCode == 200) {
        // Retrieve and deserialize the snapshot
        byte[] encodedSnapshot = ByteStreams.toByteArray(connection.getInputStream());

        SnapshotCodecV2 codec = new SnapshotCodecV2();
        TransactionSnapshot snapshot = codec.decodeState(encodedSnapshot);
        LOG.info("Snapshot taken and retrieved properly, snapshot timestamp is {} ms", snapshot.getTimestamp());

        // Persist the snapshot on disk for future queries and debugging
        String filename = hostname + ".tx.snapshot." + snapshot.getTimestamp();
        File snapshotTmpFile = new File(filename);
        Files.write(encodedSnapshot, snapshotTmpFile);
        LOG.info("Snapshot persisted on your disk as " + filename + " for future queries.");
        return snapshot;
      } else {
        LOG.error("Snapshot could not be taken. Error code: {}", responseCode);
      }
      return null;
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  private static void printSnapshotInfo(TransactionSnapshot snapshot) {
    System.out.println("==============================");
    System.out.println("= Snapshot basic information =");
    System.out.println("Current WritePtr " + txIdToDate(snapshot.getWritePointer()));
    System.out.println("Current ReadPtr " + txIdToDate(snapshot.getReadPointer()));

    printInProgressInfo(snapshot.getInProgress());
    printCommittedChangeSetsInfo(snapshot.getCommittedChangeSets());
  }

  private static void printCommittedChangeSetsInfo(Map<Long, Set<ChangeId>> committedChangeSets) {
    System.out.println("==============================");
    System.out.println("=== Committed transactions ===");
    System.out.println("Number of committed transactions to remember: " + committedChangeSets.size());

    // oldest changeset
    // biggest changeset / associated transaction (can't have the expiration)
    // sizes of the changesets
    // map of sizes with Size -> number of changesets with that size
    Map<Integer, Integer> sizes = new HashMap<Integer, Integer>();
    Map.Entry<Long, Set<ChangeId>> oldest = null,
                                   biggest = null;
    for (Map.Entry<Long, Set<ChangeId>> tx : committedChangeSets.entrySet()) {
      if (oldest == null) {
        oldest = tx;
      } else {
        if (tx.getKey() < oldest.getKey()) {
          oldest = tx;
        }
      }

      if (biggest == null) {
        biggest = tx;
      } else {
        if (tx.getValue().size() > biggest.getValue().size()) {
          biggest = tx;
        }
      }

      int currentVal = 0;
      if (sizes.containsKey(tx.getValue().size())) {
        currentVal = sizes.get(tx.getValue().size());
      }
      sizes.put(tx.getValue().size(), currentVal + 1);
    }

    if (oldest != null) {
      System.out.println("Oldest committed changeSet: " + txIdToDate(oldest.getKey()));
    }
    if (biggest != null) {
      System.out.println("Biggest committed changeSet: " + txIdToDate(biggest.getKey()));
      System.out.println("\tNumber of changes: " + biggest.getValue().size());
    }

    System.out.println("Comitted changeSets sizes:");
    for (Map.Entry<Integer, Integer> size : sizes.entrySet()) {
      System.out.println("\t" + size.getValue() + " change set(s) of size " + size.getKey());
    }
  }


  private static void printInProgressInfo(Map<Long, InMemoryTransactionManager.InProgressTx> inProgress) {
    System.out.println("==============================");
    System.out.println("== In progress transactions ==");
    System.out.println("Number of in-progress transactions: " + inProgress.size());

    long oldestLongTs = 0,
        oldestShortTs = 0;
    InMemoryTransactionManager.InProgressTx oldestLong = null,
        oldestShort = null;
    int longTxCount = 0;

    for (Map.Entry<Long, InMemoryTransactionManager.InProgressTx> tx : inProgress.entrySet()) {
      // Gather some statistics about in-progress transactions
      if (tx.getValue().isLongRunning()) {
        longTxCount++;
        if (oldestLong == null) {
          oldestLong = tx.getValue();
          oldestLongTs = tx.getKey();
        } else if (tx.getKey() < oldestLongTs) {
          oldestLong = tx.getValue();
          oldestLongTs = tx.getKey();
        }
      } else {
        if (oldestShort == null) {
          oldestShort = tx.getValue();
          oldestShortTs = tx.getKey();
        } else if (tx.getKey() < oldestShortTs) {
          oldestShort = tx.getValue();
          oldestShortTs = tx.getKey();
        }
      }
    }
    if (inProgress.size() > 0) {
      if (longTxCount > 0) {
        // Print some information about long transactions
        System.out.println("=====");
        System.out.println("Number of long transactions: " + longTxCount);
        System.out.println("Oldest long transaction" +
            "\tWritePtr " + txIdToDate(oldestLongTs) +
            "\tVisibility upper bound: " + txIdToDate(oldestLong.getVisibilityUpperBound()));
      }
      if (inProgress.size() - longTxCount > 0) {
        // Print some information about short transactions
        System.out.println("=====");
        System.out.println("Number of short transactions: " + (inProgress.size() - longTxCount));
        System.out.println("Oldest short transaction" +
            "\tWritePtr " + txIdToDate(oldestShortTs) +
            "\tExpiring at: " + formatter.format(new Date(oldestShort.getExpiration())));
        System.out.println("\tVisibility upper bound: " + txIdToDate(oldestShort.getVisibilityUpperBound()));
      }
    }
  }

  private static String txIdToDate(long id) {
    Date date = new Date(id / TxConstants.MAX_TX_PER_MS);
    return "['" + id + "' time: " + formatter.format(date) + " number: " + (id % TxConstants.MAX_TX_PER_MS) + "]";
  }

}
