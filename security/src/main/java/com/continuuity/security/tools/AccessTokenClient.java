package com.continuuity.security.tools;

import com.continuuity.common.utils.UsageException;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.Arrays;

/**
 * Client to get an AccessToken using username:password authentication.
 */
public class AccessTokenClient {
  static {
    // this turns off all logging but we don't need that for a cmdline tool
    Logger.getRootLogger().setLevel(Level.OFF);
  }

  /**
   * for debugging. should only be set to true in unit tests.
   * when true, program will print the stack trace after the usage.
   */
  public static boolean debug = false;

  boolean help = false;
  boolean verbose = false;

  String host;
  int port = -1;

  String username;
  String password;

  /**
   * Print the usage statement and return null (or empty string if this is not
   * an error case). See getValue() for an explanation of the return type.
   *
   * @param error indicates whether this was invoked as the result of an error
   * @throws com.continuuity.common.utils.UsageException
   *          in case of error
   */
  void usage(boolean error) {
    PrintStream out = (error ? System.err : System.out);
    String name = "accesstoken-client";
    if (System.getProperty("script") != null) {
      name = System.getProperty("script").replaceAll("[./]", "");
    }
    out.println("Usage: ");
    out.println("  " + name + " [ --host <host> ] [ --username <username> ]");
    out.println();
    out.println("Additional options:");
    out.println("  --host <name>           To specify the host to send to");
    out.println("  --port <number>         To specify the port to use");
    out.println("  --username <user>       To specify the user to login as");
    out.println("  --password <password>   To specify the user password");
    out.println("  --verbose               To see more verbose output");
    out.println("  --help                  To print this message");
    if (error) {
      throw new UsageException();
    }
  }

  /**
   * Print an error message followed by the usage statement.
   *
   * @param errorMessage the error message
   */
  void usage(String errorMessage) {
    if (errorMessage != null) {
      System.err.println("Error: " + errorMessage);
    }
    usage(true);
  }

  /**
   * Parse the command line arguments.
   */
  void parseArguments(String[] args) {
    if (args.length == 0) {
      usage(true);
    }
    if ("--help".equals(args[0])) {
      usage(false);
      help = true;
      return;
    }

    for (int pos = 0; pos < args.length; pos++) {
      String arg = args[pos];
      if ("--host".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        host = args[pos];
      } else if ("--port".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        try {
          port = Integer.parseInt(args[pos]);
        } catch (NumberFormatException e) {
          usage(true);
        }
      } else if ("--username".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        username = args[pos];
      } else if ("--password".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        password = args[pos];
      } else if ("--verbose".equals(arg)) {
        verbose = true;
      } else if ("--help".equals(arg)) {
        help = true;
        usage(false);
        return;
      } else {
        usage(true);
      }
    }
  }

  void validateArguments(String[] args) {
    parseArguments(args);
    if (help) {
      return;
    }
    if (host == null) {
      host = "http://localhost";
    }
    if (port == -1) {
      port = 10009;
    }

    if (username == null) {
      username = System.getProperty("user.name");
      if (username == null) {
        usage("Specify --username to login as.");
      }
    }

    //TODO: Password verification
  }

  public String execute0(String[] args) {
    validateArguments(args);
    if (help) {
      return "";
    }

    String baseUrl = String.format("%s:%d", host, port);

    HttpClient client = new DefaultHttpClient();
    HttpResponse response;

    // construct the full URL and verify its well-formedness
    try {
      URI.create(baseUrl);
    } catch (IllegalArgumentException e) {
      System.err.println("Invalid base URL '" + baseUrl + "'. Check the validity of --host or --port arguments.");
      return null;
    }

    HttpGet get = new HttpGet(baseUrl);
    String auth = Base64.encodeBase64String(String.format("%s:%s", username, password).getBytes());
    get.addHeader("Authorization", String.format("Basic %s", auth));

    try {
      response = client.execute(get);
      client.getConnectionManager().shutdown();
    } catch (IOException e) {
      System.err.println("Error sending HTTP request: " + e.getMessage());
      return null;
    }
    if (!checkHttpStatus(response)) {
      return null;
    }
    return "OK.";
  }

  /**
   * Check whether the Http return code is positive. If not, print the error
   * message and return false. Otherwise, if verbose is on, print the response
   * status line.
   *
   * @param response the HTTP response
   * @return whether the response indicates success
   */
  boolean checkHttpStatus(HttpResponse response) {
    if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
      if (verbose) {
        System.out.println(response.getStatusLine());
      } else {
        System.err.println(response.getStatusLine().getReasonPhrase());
      }
      return false;
    }
    if (verbose) {
      System.out.println(response.getStatusLine());
    }
    return true;
  }

  public String execute(String[] args) {
    try {
      return execute0(args);
    } catch (UsageException e) {
      if (debug) {
        System.err.println("Exception for arguments: " + Arrays.toString(args) + ". Exception: " + e);
        e.printStackTrace(System.err);
      }
    }
    return null;
  }

  public static void main(String[] args) {
    args = new String[] {"--username", "admin", "--password", "realtime"};
    AccessTokenClient accessTokenClient = new AccessTokenClient();
    String value = accessTokenClient.execute(args);
    if (value == null) {
      System.exit(1);
    }
  }
}
