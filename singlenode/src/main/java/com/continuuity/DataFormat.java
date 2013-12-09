package com.continuuity;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.DirUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.ServerSocket;

/**
 * Reformat the continuuity data-store, wiping out any existing user data.
 */
public class DataFormat {

  /**
   * The name of the configuration governing where data-fabric data is stored.
   */
  private static final String dataDirPropName = "data.local.jdbc";

  /**
   *  The name of the configuration governing the port which resourceMgr listens.
   *
   *  Used to confirm if the application is currently running
   */
  private static final String resourceMgrPortPropName = "resource.manager.server.port";

  /**
   * Print the usage statement and return null.
   *
   * @param error indicates whether this was invoked as the result of an error
   * @throws IllegalArgumentException in case of error
   */
  static void usage(boolean error) {
    PrintStream out = (error ? System.err : System.out);
    out.println("data-format utility:");
    out.println("  This utility will reformat the continuuity data-store," +
            " wiping out any existing user data.  The continuuity-reactor " +
            "application must be stopped before reformatting.");
    out.println("");
    out.println("Usage: ");
    out.println("  ./data-format [options]");
    out.println("");
    out.println("options:");
    out.println("  --help      To print this message");
    out.println("");

    if (error) {
      throw new IllegalArgumentException();
    }
  }

  /**
   * command-line prompt to confirm user intentions.
   *
   * @param prompt - String to display to user
   * @return - String input from user
   */
  private static String promptUser(String prompt) {
    System.out.print(prompt);

    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    String userinput = null;
    try {
      userinput = br.readLine().toLowerCase().trim();
    }  catch (IOException e) {
      e.printStackTrace();
      System.exit(-1);
    }
    return userinput;
  }

  /**
   * determine if singleNode is already running.  Do so by attempting to open a socket on the resourceMgr port.
   *
   * @param port - the port on which resourceMgr is configured to run
   * @return - true if the port was taken (singleNode is running), else false
   */
  private static boolean checkIfRunning(int port) {
    boolean portTaken = false;
    ServerSocket socket = null;
    try {
      socket = new ServerSocket(port);
      socket.setReuseAddress(true);
    } catch (IOException e) {
      portTaken = true;
    } finally {
      // Clean up
      if (socket != null) {
        try {
          socket.close();
        } catch (IOException e) {
          /* should not be thrown */
        }
      }
    }
    return portTaken;
  }


  /**
   * Main method.
   */
  public static void main(String[] args) {
    // We only support 'help' command line options currently
    if (args.length > 0) {
      if ("--help".equals(args[0]) || "-h".equals(args[0])) {
        usage(false);
        return;
      } else {
        usage(true);
      }
    }

    // first confirm user intends to delete all data
    String userinput = promptUser("Warning: this will delete any existing Continuuity user data.  Proceed? (yes/no): ");
    if (!("yes".equals(userinput) || "no".equals(userinput))) {
      // try once more
      userinput = promptUser("please type 'yes' or 'no': ");
    }

    if (!("yes".equals(userinput))) {
      System.out.println("Aborting.");
      System.exit(1);
    }

    // fetch configuration

    // This is our universal configurations object.
    CConfiguration myConfiguration = CConfiguration.create();

    // read the relevant property
    String datadirjdbc = myConfiguration.get(dataDirPropName);

    // extract the relative file path from the jdbc string
    String datadirpath = datadirjdbc.replaceAll(".*file:([^/]+)/.*", "$1");

    //String datadirpath = myConfiguration.get(dataDirPropName);
    if (datadirpath == null) {
      System.out.println("Error: cannot read data-fabric configuration. Aborting...");
      System.exit(-1);
    }

    // read the resourceMgr port configuration
    String port = myConfiguration.get(resourceMgrPortPropName);
    if (port == null) {
      System.out.println("Error: cannot read configuration. Aborting...");
      System.exit(-1);
    }

    // determine if singleNode is already running
    if (checkIfRunning(Integer.parseInt(port))) {
      System.out.println("Error: continuuity-reactor must first be stopped.");
      System.exit(-1);
    }

    // check if directory exists and process the delete
    File datadir = new File(datadirpath);
    if (!datadir.exists()) {
      System.out.println("No existing data found. Format operation unnecessary.");
      System.exit(0);
    } else {
      try {
        DirUtils.deleteDirectoryContents(datadir);
        System.out.println("Success");
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(-1);
      }
    }
  }
}
