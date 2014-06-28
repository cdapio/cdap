package com.continuuity.data2.transaction;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.transaction.inmemory.ChangeId;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.persist.TransactionSnapshot;
import com.continuuity.data2.transaction.snapshot.SnapshotCodecProvider;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Management tool to retrieve the state of the transaction manager of a reactor, and to query it.
 */
public class TransactionManagerDebuggerMain {

  private static final Gson GSON = new Gson();
  private static final SimpleDateFormat formatter
      = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S z");
  private static final String TOOL_NAME = "tx-debugger";

  private static final String HOST_OPTION = "host";
  private static final String PORT_OPTION = "port";
  private static final String FILENAME_OPTION = "filename";
  private static final String SAVE_OPTION = "save";
  private static final String IDS_OPTION = "ids";
  private static final String TRANSACTION_OPTION = "transaction";
  private static final String HELP_OPTION = "help";
  private static final String TOKEN_OPTION = "token";
  private static final String TOKEN_FILE_OPTION = "token-file";

  private enum DebuggerMode {
    VIEW,
    INVALIDATE,
    RESET,
    INVALID;

    private static DebuggerMode fromString(String str) {
      if (str.equals("view")) {
        return VIEW;
      } else if (str.equals("invalidate")) {
        return INVALIDATE;
      } else if (str.equals("reset")) {
        return RESET;
      } else {
        return INVALID;
      }
    }
  }

  private DebuggerMode mode;          // Mode the tool is used with
  private String accessToken;         // the access token for secure connections
  private String tokenFile = null;    // path to file which contains an access token
  private String hostname;            // hostname to take a snapshot from
  private String existingFilename;    // filename where a snapshot has been persisted
  private Long txId;                  // transaction ID option
  private Integer portNumber;         // port number of the router to reach
  private String persistingFilename;  // filename where the snapshot - downloaded from a hostname -
                                      // is to be persisted on the disk
  private boolean showTxids;          // show all the transaction IDs present in the snapshot

  private Options options;

  private final SnapshotCodecProvider codecProvider;

  private TransactionManagerDebuggerMain (CConfiguration configuration) {
    codecProvider = new SnapshotCodecProvider(configuration);
    buildOptions();
  }

  private void buildOptions() {
    options = new Options();
    options.addOption(null, HOST_OPTION, true, "To specify the hostname of the router");
    options.addOption(null, FILENAME_OPTION, true, "To specify a file to load a snapshot from in view mode. " +
                                                   "If the host option is specified, filename will be ignored");
    options.addOption(null, SAVE_OPTION, true, "To specify where the snapshot downloaded on hostname --host " +
                                               "should be persisted on your disk when using the view mode");
    options.addOption(null, IDS_OPTION, false, "To view all the transaction IDs contained in the " +
                                               "snapshot when using the view mode");
    options.addOption(null, TRANSACTION_OPTION, true, "To specify a transaction ID. Mandatory in invalidate mode, " +
                                                      "optional in view mode");
    options.addOption(null, PORT_OPTION, true, "To specify the port to use. The default value is --port " +
                                               Constants.Gateway.DEFAULT_PORT);
    options.addOption(null, HELP_OPTION, false, "To print this message");
    options.addOption(null, TOKEN_OPTION, true, "To specify the access token for secure connections");
    options.addOption(null, TOKEN_FILE_OPTION, true, "Alternative to --token, to specify a file that contains " +
                                                      "the access token for a secure connection");
  }

  /**
   * Parse the arguments from the command line and execute the different modes.
   * @param args command line arguments
   * @param conf default configuration
   * @return true if the arguments were parsed successfully and comply with the expected usage
   */
  private boolean parseArgsAndExecMode(String[] args, CConfiguration conf) {
    CommandLineParser parser = new GnuParser();
    // Check all the options of the command line
    try {
      CommandLine line = parser.parse(options, args);
      if (line.hasOption(HELP_OPTION)) {
        printUsage(false);
        return true;
      }

      hostname = line.getOptionValue(HOST_OPTION);
      existingFilename = line.getOptionValue(FILENAME_OPTION);
      persistingFilename = line.hasOption(SAVE_OPTION) ? line.getOptionValue(SAVE_OPTION) : null;
      showTxids = line.hasOption(IDS_OPTION);
      txId = line.hasOption(TRANSACTION_OPTION) ? Long.valueOf(line.getOptionValue(TRANSACTION_OPTION)) : null;
      accessToken = line.hasOption(TOKEN_OPTION) ? line.getOptionValue(TOKEN_OPTION).replaceAll("(\r|\n)", "") : null;
      tokenFile = line.hasOption(TOKEN_FILE_OPTION) ? line.getOptionValue(TOKEN_FILE_OPTION).replaceAll("(\r|\n)", "")
        : null;
      portNumber = line.hasOption(PORT_OPTION) ? Integer.valueOf(line.getOptionValue(PORT_OPTION)) :
                   conf.getInt(Constants.Gateway.PORT, Constants.Gateway.DEFAULT_PORT);

      // if both tokenfile and accessToken are given, just use the access token
      if (tokenFile != null) {
        if (accessToken != null) {
          tokenFile = null;
        } else {
          readTokenFile();
        }
      }

      switch (this.mode) {
        case VIEW:
          if (!line.hasOption(HOST_OPTION) && !line.hasOption(FILENAME_OPTION)) {
            usage("Either specify a hostname to download a new snapshot, " +
                  "or a filename of an existing snapshot.");
            return false;
          }
          // Execute mode
          executeViewMode();
          break;
        case INVALIDATE:
          if (!line.hasOption(HOST_OPTION) || !line.hasOption(TRANSACTION_OPTION)) {
            usage("Specify a host name and a transaction id.");
            return false;
          }
          // Execute mode
          executeInvalidateMode();
          break;
        case RESET:
          if (!line.hasOption(HOST_OPTION)) {
            usage("Specify a host name.");
            return false;
          }
          // Execute mode
          executeResetMode();
          break;
        default:
          printUsage(true);
          return false;
      }
    } catch (ParseException e) {
      printUsage(true);
      return false;
    }
    return true;
  }

  /**
   * Print an error message followed by the usage statement.
   *
   * @param errorMessage the error message
   */
  private void usage(String errorMessage) {
    if (errorMessage != null) {
      System.err.println("Error: " + errorMessage);
    }
    printUsage(true);
  }

  private void printUsage(boolean error) {
    PrintWriter pw;
    if (error) {
      pw = new PrintWriter(System.err);
    } else {
      pw = new PrintWriter(System.out);
    }
    pw.println("Usage:" +
        "\n\t " + TOOL_NAME + " view [ <option> ... ]" +
        "\n\t " + TOOL_NAME + " invalidate --host <name> --transaction <id>");
    pw.println("\nOptions:\n");
    HelpFormatter formatter = new HelpFormatter();
    formatter.printOptions(pw, 100, options, 0, 10);
    pw.flush();
    pw.close();
  }

  /**
   * Reads the access token from the tokenFile path
   */
  void readTokenFile() {
    if (tokenFile != null) {
      try {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(tokenFile));
        String line = bufferedReader.readLine();
        accessToken = line;
      } catch (FileNotFoundException e) {
        System.out.println("Could not find access token file: " + tokenFile + "\nNo access token will be used");
      } catch (IOException e) {
        System.out.println("Could not read access token file: " + tokenFile + "\nNo access token will be used");
      }
    }
  }

  private void executeViewMode() {
    TransactionSnapshot snapshot = null;
    if (hostname != null && portNumber != null) {
      // Take new snapshot and download it
      snapshot = takeSnapshot();
    } else if (existingFilename != null) {
      // Retrieve saved snapshot
      snapshot = retrieveSnapshot();
    }
    if (snapshot != null) {
      if (txId != null) {
        // Look for a particular tx id
        searchTransactionID(snapshot);
      } else {
        printSnapshotInfo(snapshot);
        if (showTxids) {
          printTxIds(snapshot);
        }
      }
    }
  }

  private void executeInvalidateMode() {
    URL url;
    HttpURLConnection connection = null;
    try {
      url = new URL("http://" + hostname + ":" + portNumber + "/v2/transactions/" + txId + "/invalidate");
      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("POST");
      if (accessToken != null) {
        connection.setRequestProperty("Authorization", "Bearer " + accessToken);
      }

      System.out.println("About to invalidate transaction " +
                          txId + " on Reactor running at " + hostname);
      int responseCode = connection.getResponseCode();
      if (responseCode == 200) {
        System.out.println("Transaction successfully invalidated.");
      } else if (responseCode == 400) {
        System.out.println("Could not invalidate transaction: " + txId + " is not a valid tx id");
      } else if (responseCode == 409) {
        System.out.println("Could not invalidate transaction " + txId + ": transaction is not in progress.");
      } else if (responseCode == 401) {
        readUnauthorizedError(connection);
      } else {
        System.out.println("Could not invalidate transaction. Error code: " + responseCode);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  private void executeResetMode() {
    URL url;
    HttpURLConnection connection = null;
    try {
      url = new URL("http://" + hostname + ":" + portNumber + "/v2/transactions/state");
      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("POST");
      if (accessToken != null) {
        connection.setRequestProperty("Authorization", "Bearer " + accessToken);
      }

      System.out.println("About to reset the transaction manager state for the Reactor running at " + hostname);
      int responseCode = connection.getResponseCode();
      if (responseCode == 200) {
        System.out.println("Transaction manager state reset successfully.");
      } else if (responseCode == 401) {
        readUnauthorizedError(connection);
      } else {
        System.out.println("Could not invalidate transaction. Error code: " + responseCode);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }
  /**
   * Look for a transaction ID in a snapshot of the transaction manager, and give all the information possible.
   * @param snapshot snapshot of the transaction manager
   */
  private void searchTransactionID(TransactionSnapshot snapshot) {
    System.out.println("Looking for transaction ID " + txId);

    InMemoryTransactionManager.InProgressTx txInfo = snapshot.getInProgress().get(txId);
    if (txInfo != null) {
      System.out.println("Transaction found in In-progress transactions:");
      System.out.println("\t" + txIdToString(txId) + " - " +
          (txInfo.isLongRunning() ? "Long" : "Short"));
      if (!txInfo.isLongRunning()) {
        System.out.println("\tExpiring at: " + formatter.format(new Date(txInfo.getExpiration())));
      }
      System.out.println("\tVisibility upper bound: " + txIdToString(txInfo.getVisibilityUpperBound()));
    }

    if (snapshot.getInvalid().contains(txId)) {
      System.out.println("Transaction found in Invalid transactions:");
      System.out.println("\t" + txIdToString(txId));
    }

    Set<ChangeId> changes = snapshot.getCommittedChangeSets().get(txId);
    if (changes != null) {
      System.out.println("Transaction found in Committed transactions:");
      System.out.println("\t" + txIdToString(txId));
      System.out.println("\tNumber of changes: " + changes.size());
      System.out.println("\tChanges: " + changes);
    }

    changes = snapshot.getCommittingChangeSets().get(txId);
    if (changes != null) {
      System.out.println("Transaction found in Committing transactions:");
      System.out.println("\t" + txIdToString(txId));
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
      try {
        TransactionSnapshot snapshot = codecProvider.decode(fis);
        System.out.println("Snapshot retrieved, timestamp is " + snapshot.getTimestamp() + " ms.");
        return snapshot;
      } finally {
        fis.close();
      }
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
      url = new URL("http://" + hostname + ":" + portNumber + "/v2/transactions/state");
      connection = (HttpURLConnection) url.openConnection();
      if (accessToken != null) {
        connection.setRequestProperty("Authorization", "Bearer " + accessToken);
      }

      System.out.println("About to take a snapshot of the transaction manager at " +
                         url.toURI() + ", timestamp is " + System.currentTimeMillis() + " ms");
      int responseCode = connection.getResponseCode();
      if (responseCode == 200) {
        // Retrieve and deserialize the snapshot
        InputStream input = connection.getInputStream();
        TransactionSnapshot snapshot;
        try {
          snapshot = codecProvider.decode(input);
        } finally {
          input.close();
        }
        System.out.println("Snapshot taken and retrieved properly, snapshot timestamp is " +
                           snapshot.getTimestamp() + " ms");

        if (persistingFilename != null) {
          // Persist the snapshot on disk for future queries and debugging
          File outputFile = new File(persistingFilename);
          OutputStream out = new FileOutputStream(outputFile);
          try {
            // todo use pipes here to avoid having everyhting in memory twice
            codecProvider.encode(out, snapshot);
          } finally {
            out.close();
          }
          System.out.println("Snapshot persisted on your disk as " + outputFile.getAbsolutePath() +
                             " for future queries.");
        } else {
          System.out.println("Persist option not activated - Snapshot won't be persisted on your disk.");
        }
        return snapshot;
      } else if (responseCode == 401) {
        readUnauthorizedError(connection);
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
        System.out.println("Oldest long transaction:" +
                           "\n\tWritePtr " + txIdToString(oldestLong.getKey()) +
                           "\n\tVisibility upper bound: " +
                           txIdToString(oldestLong.getValue().getVisibilityUpperBound()));
      }
      if (inProgress.size() - longTxCount > 0) {
        // Print some information about short transactions
        System.out.println("=====");
        System.out.println("Number of short transactions: " + (inProgress.size() - longTxCount));
        System.out.println("Average age of short transactions: " +
            formatter.format(new Date(avgShortAge / (inProgress.size() - longTxCount))));
        System.out.println("Oldest short transaction:" +
                           "\n\tWritePtr " + txIdToString(oldestShort.getKey()) +
                           "\n\tExpiring at: " + formatter.format(new Date(oldestShort.getValue().getExpiration())) +
                           "\n\tVisibility upper bound: " +
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
    if (args.length <= 0) {
      printUsage(true);
      return false;
    }
    mode = DebuggerMode.fromString(args[0]);
    if (mode == DebuggerMode.INVALID) {
      printUsage(true);
      return false;
    }
    List<String> subArgs = Arrays.asList(args).subList(1, args.length);
    return parseArgsAndExecMode(subArgs.toArray(new String[0]), conf);
  }

  /**
   * Prints the error response from the connection
   * @param connection the connection to read the response from
   */
  private void readUnauthorizedError(HttpURLConnection connection) {
    System.out.println("401 Unauthorized");
    if (accessToken == null) {
      System.out.println("No access token provided");
      return;
    }
    Reader reader = null;
    try {
      reader = new InputStreamReader(connection.getErrorStream());
      String responseError = GSON.fromJson(reader, ErrorMessage.class).getErrorDescription();
      if (responseError != null && !responseError.isEmpty()) {
        System.out.println(responseError);
      }
    } catch (Exception e) {
      System.out.println("Unknown unauthorized error");
    }
  }

  /**
   * Error Description from HTTPResponse
   */
  private class ErrorMessage {
    @SerializedName("error_description")
    private String errorDescription;

    public String getErrorDescription() {
      return errorDescription;
    }
  }

  public static void main(String[] args) {
    // create a config and load the gateway properties
    CConfiguration config = CConfiguration.create();
    TransactionManagerDebuggerMain instance = new TransactionManagerDebuggerMain(config);
    boolean success = instance.execute(args, config);
    if (!success) {
      System.exit(1);
    }
  }
}
