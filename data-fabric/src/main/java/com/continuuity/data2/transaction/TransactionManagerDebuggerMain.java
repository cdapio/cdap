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
import java.util.Collection;
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
 * Management tool to retrieve the state of the transaction manager of a reactor, and to query it.
 */
public class TransactionManagerDebuggerMain {

  private static final SimpleDateFormat formatter
      = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S z");

  private static final Logger LOG =
      LoggerFactory.getLogger(TransactionManagerDebuggerMain.class);

  public static void main(String[] args) {
    CommandLineParser parser = new GnuParser();
    Options options = new Options();
    options.addOption("h", "hostname", true, "Hostname");
    options.addOption("f", "existingFile", true, "Existing snapshot");
    options.addOption("o", "oneShot", false, "Do not persist the snapshot");
    options.addOption("ids", "txIds", false, "Show transaction IDs");
    options.addOption("id", "txSearched", true, "Look for a transaction given a transaction ID");

    String hostname;
    String existingFilename;
    Long txIdToSearch;
    boolean persistSnapshot;
    boolean showTxids;

    // Check all the options of command line
    try {
      CommandLine line = parser.parse(options, args);
      if (!line.hasOption("hostname") && !line.hasOption("existingFile")) {
        LOG.error("Either specify a hostname to retrieve a snapshot from, or an existing snapshot.");
        return;
      }

      hostname = line.getOptionValue("hostname");
      existingFilename = line.getOptionValue("existingFile");
      persistSnapshot = line.hasOption("oneShot") ? false : true;
      showTxids = line.hasOption("txIds") ? true : false;
      txIdToSearch = line.hasOption("txSearched") ? Long.valueOf(line.getOptionValue("txSearched")) : null;
    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("SandboxJVM", options);
      return;
    }

    TransactionSnapshot snapshot = null;
    if (hostname != null) {
      // Take new snapshot and download it
      snapshot = takeSnapshot(hostname, persistSnapshot);
    } else if (existingFilename != null) {
      // Retrieve saved snapshot
      snapshot = retrieveSnapshot(existingFilename);
    }

    if (snapshot != null) {
      if (txIdToSearch != null) {
        // Look for a particular tx id
        searchTransactionID(snapshot, txIdToSearch);
      } else {
        printSnapshotInfo(snapshot);
        if (showTxids) {
          printTxIds(snapshot);
        }
      }
    }
  }

  /**
   * Look for a transaction ID in a snapshot of the transaction manager, and give all the information possible.
   * @param snapshot snapshot of the transaction manager
   * @param txIdToSearch transaction ID to look for in the snapshot
   */
  private static void searchTransactionID(TransactionSnapshot snapshot, Long txIdToSearch) {
    System.out.println("Looking for transaction ID " + txIdToSearch);

    InMemoryTransactionManager.InProgressTx txInfo = snapshot.getInProgress().get(txIdToSearch);
    if (txInfo != null) {
      System.out.println("Transaction found in In-progress transactions :");
      System.out.println("\t" + txIdToDate(txIdToSearch) + " - " +
          (txInfo.isLongRunning() ? "Long" : "Short"));
      if (!txInfo.isLongRunning()) {
        System.out.println("\tExpiring at: " + formatter.format(new Date(txInfo.getExpiration())));
      }
      System.out.println("\tVisibility upper bound: " + txIdToDate(txInfo.getVisibilityUpperBound()));
    }

    if (snapshot.getInvalid().contains(txIdToSearch)) {
      System.out.println("Transaction found in Invalid transactions :");
      System.out.println("\t" + txIdToDate(txIdToSearch));
    }

    Set<ChangeId> changes = snapshot.getCommittedChangeSets().get(txIdToSearch);
    if (changes != null) {
      System.out.println("Transaction found in Committed transactions :");
      System.out.println("\t" + txIdToDate(txIdToSearch));
      System.out.println("\tNumber of changes: " + changes.size());
      System.out.println("\tChanges: " + changes);
    }

    changes = snapshot.getCommittingChangeSets().get(txIdToSearch);
    if (changes != null) {
      System.out.println("Transaction found in Committing transactions :");
      System.out.println("\t" + txIdToDate(txIdToSearch));
      System.out.println("\tNumber of changes: " + changes.size());
      System.out.println("\tChanges: " + changes);
    }
  }

  /**
   * Retrieve a persisted snapshot taken in the past.
   * @param filename path to the snapshot file
   * @return the decoded transaction manager snapshot
   */
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

  /**
   * Take a snapshot from the transaction manager of a reactor and retrieve it.
   * @param hostname hostname of the targeted reactor
   * @param persistSnapshot save the snapshot as a file on the disk
   * @return the transaction manager snapshot just taken
   */
  private static TransactionSnapshot takeSnapshot(String hostname, boolean persistSnapshot) {
    URL url;
    HttpURLConnection connection = null;
    try {
      url = new URL("http://" + hostname + ":10000/v2/transactions/snapshot");
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

        if (persistSnapshot) {
          // Persist the snapshot on disk for future queries and debugging
          String filename = hostname + ".tx.snapshot." + snapshot.getTimestamp();
          File snapshotTmpFile = new File(filename);
          Files.write(encodedSnapshot, snapshotTmpFile);
          LOG.info("Snapshot persisted on your disk as " + filename + " for future queries.");
        } else {
          LOG.info("One shot option activated - Snapshot won't be persisted on your disk.");
        }
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

  /**
   * Print basic information and statistics about a transaction manager snapshot.
   * @param snapshot transaction manager snapshot
   */
  private static void printSnapshotInfo(TransactionSnapshot snapshot) {
    System.out.println("==============================");
    System.out.println("= Snapshot basic information =");
    System.out.println("Snapshot timestamp is " + formatter.format(new Date(snapshot.getTimestamp())));
    System.out.println("Current WritePtr " + txIdToDate(snapshot.getWritePointer()));
    System.out.println("Current ReadPtr " + txIdToDate(snapshot.getReadPointer()));

    printInProgressInfo(snapshot.getInProgress());
    printInvalidInfo(snapshot.getInvalid());
    printChangeSetsInfo(snapshot.getCommittedChangeSets(), true);
    printChangeSetsInfo(snapshot.getCommittingChangeSets(), false);
  }

  /**
   * Print basic information and statistics about invalid transactions.
   * @param invalids collection of invalid transaction IDs
   */
  private static void printInvalidInfo(Collection<Long> invalids) {
    System.out.println("==============================");
    System.out.println("==== Invalid transactions ====");
    System.out.println("Number of invalid transactions: " + invalids.size());

    long oldest = 0;
    long avgAge = 0;
    for (long tx: invalids) {
      // Gather some statistics about invalid transactions
      avgAge += tx / TxConstants.MAX_TX_PER_MS;
      if (oldest == 0) {
        oldest = tx;
      } else if (tx < oldest) {
        oldest = tx;
      }
    }
    if (invalids.size() > 0) {
      System.out.println("Average age of invalid transactions: "
          + formatter.format(new Date(avgAge / invalids.size())));
      System.out.println("Oldest invalid transaction " + txIdToDate(oldest));
    }
  }

  /**
   * Print basic information and statistics about committed/committing transactions.
   * @param changeSets transaction IDs associated to their changes
   * @param committed true if committed change sets, false if committing change sets
   */
  private static void printChangeSetsInfo(Map<Long, Set<ChangeId>> changeSets, boolean committed) {
    String term = committed ? "committed" : "committing";

    System.out.println("==============================");
    System.out.println("=== Transactions " + term + " ===");
    System.out.println("Number of " + term + " transactions: " + changeSets.size());

    // oldest changeset
    // biggest changeset / associated transaction (can't have the expiration)
    // sizes of the changesets

    // map of sizes with Size -> number of changesets with that size
    Map<Integer, Integer> sizes = new HashMap<Integer, Integer>();
    Map.Entry<Long, Set<ChangeId>> oldest = null,
                                   biggest = null;
    for (Map.Entry<Long, Set<ChangeId>> tx : changeSets.entrySet()) {
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
      System.out.println("Oldest " + term + " changeSet: " + txIdToDate(oldest.getKey()));
      System.out.println("\tNumber of changes: " + oldest.getValue().size());
      System.out.println("\tChanges: " + oldest.getValue());
    }
    if (biggest != null) {
      System.out.println("Biggest " + term + " changeSet: " + txIdToDate(biggest.getKey()));
      System.out.println("\tNumber of changes: " + biggest.getValue().size());
      System.out.println("\tChanges: " + biggest.getValue());
    }

    System.out.println(term + " changeSets sizes:");
    for (Map.Entry<Integer, Integer> size : sizes.entrySet()) {
      System.out.println("\t" + size.getValue() + " change set(s) of size " + size.getKey());
    }
  }

  /**
   * Print basic information and statistics about in-progress transactions.
   * @param inProgress in progress transaction IDs mapped to information about those
   */
  private static void printInProgressInfo(Map<Long, InMemoryTransactionManager.InProgressTx> inProgress) {
    System.out.println("==============================");
    System.out.println("== In progress transactions ==");
    System.out.println("Number of in-progress transactions: " + inProgress.size());

    Map.Entry<Long, InMemoryTransactionManager.InProgressTx> oldestLong = null, oldestShort = null;

    int longTxCount = 0;
    long avgLongAge = 0, avgShortAge = 0;
    for (Map.Entry<Long, InMemoryTransactionManager.InProgressTx> tx : inProgress.entrySet()) {
      // Gather some statistics about in-progress transactions
      if (tx.getValue().isLongRunning()) {
        longTxCount++;
        avgLongAge += tx.getKey() / TxConstants.MAX_TX_PER_MS;
        if (oldestLong == null) {
          oldestLong = tx;
        } else if (tx.getKey() < oldestLong.getKey()) {
          oldestLong = tx;
        }
      } else {
        avgShortAge += tx.getKey() / TxConstants.MAX_TX_PER_MS;
        if (oldestShort == null) {
          oldestShort = tx;
        } else if (tx.getKey() < oldestShort.getKey()) {
          oldestShort = tx;
        }
      }
    }
    if (inProgress.size() > 0) {
      if (longTxCount > 0) {
        // Print some information about long transactions
        System.out.println("=====");
        System.out.println("Number of long transactions: " + longTxCount);
        System.out.println("Average age of long transactions: " + formatter.format(new Date(avgLongAge / longTxCount)));
        System.out.println("Oldest long transaction" +
            "\tWritePtr " + txIdToDate(oldestLong.getKey()) +
            "\tVisibility upper bound: " + txIdToDate(oldestLong.getValue().getVisibilityUpperBound()));
      }
      if (inProgress.size() - longTxCount > 0) {
        // Print some information about short transactions
        System.out.println("=====");
        System.out.println("Number of short transactions: " + (inProgress.size() - longTxCount));
        System.out.println("Average age of short transactions: " +
            formatter.format(new Date(avgShortAge / (inProgress.size() - longTxCount))));
        System.out.println("Oldest short transaction" +
            "\tWritePtr " + txIdToDate(oldestShort.getKey()) +
            "\tExpiring at: " + formatter.format(new Date(oldestShort.getValue().getExpiration())));
        System.out.println("\tVisibility upper bound: " + txIdToDate(oldestShort.getValue().getVisibilityUpperBound()));
      }
    }
  }

  /**
   * Print all the transaction IDs found in the transaction manager snapshot.
   * @param snapshot transaction manager snapshot
   */
  private static void printTxIds(TransactionSnapshot snapshot) {
    System.out.println("\n======================================");
    System.out.println("======== All transaction Ids =========");

    System.out.println("=== In progress transactions ===");
    for (Map.Entry<Long, InMemoryTransactionManager.InProgressTx> tx : snapshot.getInProgress().entrySet()) {
      System.out.println(txIdToDate(tx.getKey()) + " - " +
          (tx.getValue().isLongRunning() ? "Long" : "Short"));
    }

    System.out.println("=== Invalid transactions ===");
    for (long tx : snapshot.getInvalid()) {
      System.out.println(txIdToDate(tx));
    }

    System.out.println("=== Committed transactions ===");
    for (Map.Entry<Long, Set<ChangeId>> tx : snapshot.getCommittedChangeSets().entrySet()) {
      System.out.println(txIdToDate(tx.getKey()));
    }

    System.out.println("=== Committing transactions ===");
    for (Map.Entry<Long, Set<ChangeId>> tx : snapshot.getCommittingChangeSets().entrySet()) {
      System.out.println(txIdToDate(tx.getKey()));
    }
  }

  /**
   * Utility method to convert a transaction ID to its string representation.
   * @param id transaction ID to convert.
   * @return string representation of a transaction ID.
   */
  private static String txIdToDate(long id) {
    Date date = new Date(id / TxConstants.MAX_TX_PER_MS);
    return "['" + id + "' time: " + formatter.format(date) + " number: " + (id % TxConstants.MAX_TX_PER_MS) + "]";
  }

}
