package com.continuuity.data2.transaction;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.Decoder;
import com.continuuity.data2.transaction.inmemory.ChangeId;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.persist.SnapshotCodecV2;
import com.continuuity.data2.transaction.persist.TransactionSnapshot;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

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
  private static final String TOOL_NAME = "transactions-debugger";

  private String hostname;            // hostname to take a snapshot from
  private String existingFilename;    // filename where a snapshot has been persisted
  private Long txIdToSearch;          // transaction ID to look for in the snapshot
  private Integer portNumber;         // port number of the router to reach
  private String persistingFilename;  // filename where the snapshot - downloaded from a hostname -
                                      // is to be persisted on the disk
  private boolean showTxids;          // show all the transaction IDs present in the snapshot

  private TransactionManagerDebuggerMain () {
  }

  /**
   * Parse the arguments from the command line.
   * @param args command line arguments
   * @param conf default configuration
   * @return True if the arguments where parsed successfully and comply with the expected usage
   */
  private boolean parseArguments(String[] args, CConfiguration conf) {
    CommandLineParser parser = new GnuParser();
    Options options = new Options();
    options.addOption(null, "host", true, "To specify the hostname of the router");
    options.addOption(null, "filename", true, "To specify a file to load where a snapshot is saved");
    options.addOption(null, "save", true, "To specify where the snapshot downloaded on " +
                                          "hostname --host should be persisted on your disk");
    options.addOption(null, "ids", false, "To view all the transaction IDs contained in the snapshot");
    options.addOption(null, "transaction", true, "To specify a transaction ID to look for");
    options.addOption(null, "port", true, "To specify the port to use. The default value is --port " +
                                          Constants.Gateway.DEFAULT_PORT);
    options.addOption(null, "help", false, "To print this message");

    // Check all the options of the command line
    try {
      CommandLine line = parser.parse(options, args);
      if (line.hasOption("help")) {
        printUsage(options, false);
        return true;
      }
      if (!line.hasOption("host") && !line.hasOption("filename")) {
        usage(options, "Either specify a hostname to download a new snapshot, " +
                       "or a filename of an existing snapshot.");
        return false;
      }

      hostname = line.getOptionValue("host");
      existingFilename = line.getOptionValue("filename");
      persistingFilename = line.hasOption("save") ? line.getOptionValue("save") : null;
      showTxids = line.hasOption("ids") ? true : false;
      portNumber = line.hasOption("port") ? Integer.valueOf(line.getOptionValue("port")) :
                   conf.getInt(Constants.Gateway.PORT, Constants.Gateway.DEFAULT_PORT);
      txIdToSearch = line.hasOption("transaction") ? Long.valueOf(line.getOptionValue("transaction")) : null;
    } catch (ParseException e) {
      printUsage(options, true);
      return false;
    }
    return true;
  }

  /**
   * Print an error message followed by the usage statement.
   *
   * @param options the options to show
   * @param errorMessage the error message
   */
  private static void usage(Options options, String errorMessage) {
    if (errorMessage != null) {
      System.err.println("Error: " + errorMessage);
    }
    printUsage(options, true);
  }

  private static void printUsage(Options options, boolean error) {
    PrintWriter pw;
    if (error) {
      pw = new PrintWriter(System.err);
    } else {
      pw = new PrintWriter(System.out);
    }
    pw.println("Usage:" +
        "\n\t " + TOOL_NAME + " --host <name> [ <option> ... ]" +
        "\n\t " + TOOL_NAME + " --filename <filename> [ <option> ... ]");
    pw.println("\nOptions:\n");
    HelpFormatter formatter = new HelpFormatter();
    formatter.printOptions(pw, 100, options, 0, 10);
    pw.flush();
    pw.close();
  }

  /**
   * Look for a transaction ID in a snapshot of the transaction manager, and give all the information possible.
   * @param snapshot snapshot of the transaction manager
   */
  private void searchTransactionID(TransactionSnapshot snapshot) {
    System.out.println("Looking for transaction ID " + txIdToSearch);

    InMemoryTransactionManager.InProgressTx txInfo = snapshot.getInProgress().get(txIdToSearch);
    if (txInfo != null) {
      System.out.println("Transaction found in In-progress transactions:");
      System.out.println("\t" + txIdToString(txIdToSearch) + " - " +
          (txInfo.isLongRunning() ? "Long" : "Short"));
      if (!txInfo.isLongRunning()) {
        System.out.println("\tExpiring at: " + formatter.format(new Date(txInfo.getExpiration())));
      }
      System.out.println("\tVisibility upper bound: " + txIdToString(txInfo.getVisibilityUpperBound()));
    }

    if (snapshot.getInvalid().contains(txIdToSearch)) {
      System.out.println("Transaction found in Invalid transactions:");
      System.out.println("\t" + txIdToString(txIdToSearch));
    }

    Set<ChangeId> changes = snapshot.getCommittedChangeSets().get(txIdToSearch);
    if (changes != null) {
      System.out.println("Transaction found in Committed transactions:");
      System.out.println("\t" + txIdToString(txIdToSearch));
      System.out.println("\tNumber of changes: " + changes.size());
      System.out.println("\tChanges: " + changes);
    }

    changes = snapshot.getCommittingChangeSets().get(txIdToSearch);
    if (changes != null) {
      System.out.println("Transaction found in Committing transactions:");
      System.out.println("\t" + txIdToString(txIdToSearch));
      System.out.println("\tNumber of changes: " + changes.size());
      System.out.println("\tChanges: " + changes);
    }
  }

  /**
   * Retrieve a persisted snapshot taken in the past.
   * @return the decoded transaction manager snapshot
   */
  private TransactionSnapshot retrieveSnapshot() {
    try {
      System.out.println("Retrieving snapshot from file " + existingFilename);
      File snapshotFile = new File(existingFilename);
      FileInputStream fis = new FileInputStream(snapshotFile);
      SnapshotCodecV2 codec = new SnapshotCodecV2();
      TransactionSnapshot snapshot = codec.decodeState(fis);
      System.out.println("Snapshot retrieved, timestamp is " + snapshot.getTimestamp() + " ms.");
      return snapshot;
    } catch (IOException e) {
      System.out.println("File " + existingFilename + " could not be read.");
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Take a snapshot from the transaction manager of a reactor and retrieve it.
   * @return the transaction manager snapshot just taken
   */
  private TransactionSnapshot takeSnapshot() {
    URL url;
    HttpURLConnection connection = null;
    try {
      url = new URL("http://" + hostname + ":" + portNumber + "/v2/transactions/snapshot");
      connection = (HttpURLConnection) url.openConnection();

      System.out.println("About to take a snapshot of the transaction manager at " +
          url.toURI() + ", timestamp is " + System.currentTimeMillis() + " ms");
      int responseCode = connection.getResponseCode();
      if (responseCode == 200) {
        // Retrieve and deserialize the snapshot
        SnapshotCodecV2 codec = new SnapshotCodecV2();

        TransactionSnapshot snapshot = codec.decodeState(connection.getInputStream());
        System.out.println("Snapshot taken and retrieved properly, snapshot timestamp is " +
            snapshot.getTimestamp() + " ms");

        if (persistingFilename != null) {
          // Persist the snapshot on disk for future queries and debugging
          FileOutputStream fos = null;
          File outputFile = null;
          try {
            // todo use pipes here to avoid having everyhting in memory twice
            outputFile = new File(persistingFilename);
            fos = new FileOutputStream(outputFile);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            codec.encodeState(baos, snapshot);
            baos.writeTo(fos);
          } finally {
            fos.close();
          }
          System.out.println("Snapshot persisted on your disk as " + outputFile.getAbsolutePath() +
                             " for future queries.");
        } else {
          System.out.println("Persist option not activated - Snapshot won't be persisted on your disk.");
        }
        return snapshot;
      } else {
        System.out.println("Snapshot could not be taken. Error code: " + responseCode);
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
  private void printSnapshotInfo(TransactionSnapshot snapshot) {
    System.out.println("==============================");
    System.out.println("= Snapshot basic information =");
    System.out.println("Snapshot timestamp is " + formatter.format(new Date(snapshot.getTimestamp())));
    System.out.println("Current WritePtr " + txIdToString(snapshot.getWritePointer()));
    System.out.println("Current ReadPtr " + txIdToString(snapshot.getReadPointer()));

    printInProgressInfo(snapshot.getInProgress());
    printInvalidInfo(snapshot.getInvalid());
    printChangeSetsInfo(snapshot.getCommittedChangeSets(), true);
    printChangeSetsInfo(snapshot.getCommittingChangeSets(), false);
  }

  /**
   * Print basic information and statistics about invalid transactions.
   * @param invalids collection of invalid transaction IDs
   */
  private void printInvalidInfo(Collection<Long> invalids) {
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
      System.out.println("Oldest invalid transaction " + txIdToString(oldest));
    }
  }

  /**
   * Print basic information and statistics about committed/committing transactions.
   * @param changeSets transaction IDs associated to their changes
   * @param committed true if committed change sets, false if committing change sets
   */
  private void printChangeSetsInfo(Map<Long, Set<ChangeId>> changeSets, boolean committed) {
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
      if (oldest == null || tx.getKey() < oldest.getKey()) {
        oldest = tx;
      }
      if (biggest == null || tx.getValue().size() > biggest.getValue().size()) {
        biggest = tx;
      }
      int currentVal = 0;
      if (sizes.containsKey(tx.getValue().size())) {
        currentVal = sizes.get(tx.getValue().size());
      }
      sizes.put(tx.getValue().size(), currentVal + 1);
    }

    if (oldest != null) {
      System.out.println("Oldest " + term + " changeSet: " + txIdToString(oldest.getKey()));
      System.out.println("\tNumber of changes: " + oldest.getValue().size());
      System.out.println("\tChanges: " + oldest.getValue());
    }
    if (biggest != null) {
      System.out.println("Biggest " + term + " changeSet: " + txIdToString(biggest.getKey()));
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
  private void printInProgressInfo(Map<Long, InMemoryTransactionManager.InProgressTx> inProgress) {
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
        if (oldestLong == null || tx.getKey() < oldestLong.getKey()) {
          oldestLong = tx;
        }
      } else {
        avgShortAge += tx.getKey() / TxConstants.MAX_TX_PER_MS;
        if (oldestShort == null || tx.getKey() < oldestShort.getKey()) {
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
            "\tWritePtr " + txIdToString(oldestLong.getKey()) +
            "\tVisibility upper bound: " + txIdToString(oldestLong.getValue().getVisibilityUpperBound()));
      }
      if (inProgress.size() - longTxCount > 0) {
        // Print some information about short transactions
        System.out.println("=====");
        System.out.println("Number of short transactions: " + (inProgress.size() - longTxCount));
        System.out.println("Average age of short transactions: " +
            formatter.format(new Date(avgShortAge / (inProgress.size() - longTxCount))));
        System.out.println("Oldest short transaction" +
            "\tWritePtr " + txIdToString(oldestShort.getKey()) +
            "\tExpiring at: " + formatter.format(new Date(oldestShort.getValue().getExpiration())));
        System.out.println("\tVisibility upper bound: " +
            txIdToString(oldestShort.getValue().getVisibilityUpperBound()));
      }
    }
  }

  /**
   * Print all the transaction IDs found in the transaction manager snapshot.
   * @param snapshot transaction manager snapshot
   */
  private void printTxIds(TransactionSnapshot snapshot) {
    System.out.println("\n======================================");
    System.out.println("======== All transaction Ids =========");

    System.out.println("=== In progress transactions ===");
    for (Map.Entry<Long, InMemoryTransactionManager.InProgressTx> tx : snapshot.getInProgress().entrySet()) {
      System.out.println(txIdToString(tx.getKey()) + " - " +
          (tx.getValue().isLongRunning() ? "Long" : "Short"));
    }

    System.out.println("=== Invalid transactions ===");
    for (long tx : snapshot.getInvalid()) {
      System.out.println(txIdToString(tx));
    }

    System.out.println("=== Committed transactions ===");
    for (Map.Entry<Long, Set<ChangeId>> tx : snapshot.getCommittedChangeSets().entrySet()) {
      System.out.println(txIdToString(tx.getKey()));
    }

    System.out.println("=== Committing transactions ===");
    for (Map.Entry<Long, Set<ChangeId>> tx : snapshot.getCommittingChangeSets().entrySet()) {
      System.out.println(txIdToString(tx.getKey()));
    }
  }

  /**
   * Utility method to convert a transaction ID to its string representation.
   * @param id transaction ID to convert.
   * @return string representation of a transaction ID.
   */
  private String txIdToString(long id) {
    Date date = new Date(id / TxConstants.MAX_TX_PER_MS);
    return "['" + id + "' start time: " + formatter.format(date) + " number: " + (id % TxConstants.MAX_TX_PER_MS) + "]";
  }

  private boolean execute(String[] args, CConfiguration conf) {
    boolean success = parseArguments(args, conf);
    if (!success) {
      return false;
    }
    TransactionSnapshot snapshot = null;
    if (hostname != null && portNumber != null) {
      // Take new snapshot and download it
      snapshot = takeSnapshot();
    } else if (existingFilename != null) {
      // Retrieve saved snapshot
      snapshot = retrieveSnapshot();
    }

    if (snapshot != null) {
      if (txIdToSearch != null) {
        // Look for a particular tx id
        searchTransactionID(snapshot);
      } else {
        printSnapshotInfo(snapshot);
        if (showTxids) {
          printTxIds(snapshot);
        }
      }
    }
    return true;
  }

  public static void main(String[] args) {
    // create a config and load the gateway properties
    CConfiguration config = CConfiguration.create();
    TransactionManagerDebuggerMain instance = new TransactionManagerDebuggerMain();
    boolean success = instance.execute(args, config);
    if (!success) {
      System.exit(1);
    }
  }
}
