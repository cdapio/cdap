package com.continuuity.gateway.tools;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.UsageException;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.log4j.Level;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Base class for reactor tools
 */
public abstract class ClientToolBase {

  /**
   * for debugging. should only be set to true in unit tests.
   * when true, program will print the stack trace after the usage.
   */
  public static boolean debug = false;

  static {
    // this turns off all logging but we don't need that for a cmdline tool
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.OFF);
  }

  private String toolName;
  protected static final Gson GSON = new Gson();

  private static final String HOST_OPTION = "host";
  private static final String PORT_OPTION = "port";
  private static final String HELP_OPTION = "help";
  private static final String VERBOSE_OPTION = "verbose";
  private static final String API_KEY_OPTION = "apikey";
  private static final String TOKEN_FILE_OPTION = "token-file";
  private static final String TOKEN_OPTION = "token";

  protected Options options;

  protected boolean help = false;
  protected boolean verbose = false;
  protected boolean forceNoSSL = false;
  protected String apikey = null;          // the api key for authentication
  protected String tokenFile = null;       // the file containing the access token only
  protected String accessToken = null;     // the access token for secure connections
  protected String hostname = null;
  protected int port = -1;

  /**
   * A base class for implementing reactor tools.
   *
   * @param toolName The name of the tool.
   */
  public ClientToolBase(String toolName) {
    this.toolName = toolName;
  }

  /**
   * Returns the name of the tool.
   *
   * @return tool name
   */
  public String getToolName() {
    return this.toolName;
  }

  /**
   * Forces no SSL.
   *
   * @return Returns this instance.
   */
  public ClientToolBase disallowSSL() {
    this.forceNoSSL = true;
    return this;
  }

  /**
   * Adds all basic options for a tool as well as additional options as specified by the addOptions method.
   */
  private void buildOptions() {
    options = new Options();
    options.addOption(null, HOST_OPTION, true, "To specify the reactor host");
    options.addOption(null, PORT_OPTION, true, "To specify the port to use. The default value is --port "
                      + Constants.Gateway.DEFAULT_PORT);
    options.addOption(null, HELP_OPTION, false, "To print this message");
    options.addOption(null, VERBOSE_OPTION, false, "Prints verbose output");
    options.addOption(null, API_KEY_OPTION, true, "To specify an API key for authentication");
    options.addOption(null, TOKEN_OPTION, true, "To specify the access token for secure reactor");
    options.addOption(null, TOKEN_FILE_OPTION, true, "To specify a path to the access token for secure reactor");
    addOptions(options);
  }

  /**
   * Parses the basic arguments which are common to all tools.
   *
   * @param line The instance of CommandLine which has already parsed the arguments.
   */
  private void parseBasicArgs(CommandLine line) {
    if (line.hasOption(HELP_OPTION)) {
      printUsage(false);
      help = true;
      return;
    }
    verbose = line.hasOption(VERBOSE_OPTION);
    hostname = line.getOptionValue(HOST_OPTION, null);
    port = line.hasOption(PORT_OPTION) ? parseNumericArg(line, PORT_OPTION).intValue() : -1;
    apikey = line.hasOption(API_KEY_OPTION) ? line.getOptionValue(API_KEY_OPTION) : null;
    accessToken = line.hasOption(TOKEN_OPTION) ? line.getOptionValue(TOKEN_OPTION).replaceAll("(\r|\n)", "") : null;
    tokenFile = line.getOptionValue(TOKEN_FILE_OPTION, null);
    // read the access token if one is not provided but the access token file is provided.
    if (accessToken == null && tokenFile != null) {
      accessToken = readTokenFile();
    }
  }

  /**
   * Specifies any additional options that could appear in the tool.
   * Should be overridden by child classes to add new optional arguments.
   *
   * @param options The Options object to add to.
   */
  protected void addOptions(Options options) { }

  /**
   * Parses args based on the options that were added when buildOptions was called.
   *
   * @param args The array of arguments to parse.
   * @return Returns true if the parsing succeeded and the help option was not specified.
   */
  protected boolean parseArguments(String[] args) {
    // parse generic args first
    CommandLineParser parser = new BasicParser();
    // Check all the options of the command line
    try {
      CommandLine line = parser.parse(options, args);
      parseBasicArgs(line);
      if (help) {
        return false;
      }
      return parseAdditionalArguments(line);
    } catch (ParseException e) {
      printUsage(true);
    } catch (IndexOutOfBoundsException e) {
      printUsage(true);
    }
    return true;
  }

  /**
   * Used to validate all additional arguments added by child class. Should return
   * the error message that should be displayed along with the usage if there are
   * invalid arguments.
   *
   * @return The error message to display along with the usage. Null if the usage should not be printed.
   */
  protected String validateArguments() { return null; }

  /**
   * Should be implemented by child classes to run the main logic. In order to make it testable,
   * instead of exiting in case of error it returns null, whereas in case of
   * success it returns the retrieved value as shown on the console.
   *
   * @param config The configuration of the gateway
   * @return null in case of error. A String representing the retrieved value
   * in case of success
   */
  protected abstract String execute(CConfiguration config);

  /**
   * Should be overridden by child classes to parse additional arguments if needed. This will often
   * go hand in hand with overriding addOptions. For positional arguments, first parse all optional
   * arguments and then check the remaining arguments in the line object using getArgs.
   *
   * @param line The CommandLine object which contains all arguments that have been passed in.
   * @return A boolean indicating whether or not the parsing was successful/valid or not.
   */
  protected boolean parseAdditionalArguments(CommandLine line) { return true; }

  /**
   * The main execute method for all tools. Parses the arguments, checks that they are valid,
   * and then calls the child class's execute method. In order to make it testable,
   * instead of exiting in case of error it returns null, whereas in case of
   * success it returns the retrieved value as shown on the console.
   *
   * @param args The array of arguments to parse.
   * @param config The configuration of the gateway
   * @return null in case of error. A String representing the retrieved value
   * in case of success
   */
  public String execute(String[] args, CConfiguration config) {
    try {
      buildOptions();
      boolean parseResult = parseArguments(args);
      if (help) {
        return "";
      }
      if (!parseResult) {
        printUsage(true);
        return null;
      }

      String usageMessage = validateArguments();
      if (usageMessage != null) {
        usage(usageMessage);
      }
      return execute(config);
    } catch (UsageException e) {
      if (debug) { // this is mainly for debugging the unit test
        System.err.println("Exception for arguments: " + Arrays.toString(args) + ". Exception: " + e);
        e.printStackTrace(System.err);
      }
    }
    return null;
  }

  /**
   * Should be overridden if any information should be printed before the
   * available command line options like positional arguments.
   *
   * @param error Used to print to different streams, System.out vs System.err
   */
  protected void printUsageTop(boolean error) {
    PrintStream out = error ? System.err : System.out;
    out.println(toolName + " Usage: ");
  }

  /**
   * Sends http requests with apikey and access token headers
   * and checks the status of the request afterwards.
   *
   * @param requestBase The request to send. This method adds the apikey and access token headers
   *                    if they are valid.
   * @param expectedCodes The list of expected status codes from the request. If set to null,
   *                      this method checks that the status code is OK.
   * @return The HttpResponse if the request was successfully sent and the request status code
   * is one of expectedCodes or OK if expectedCodes is null. Otherwise, returns null.
   */
  protected HttpResponse sendHttpRequest(HttpClient client, HttpRequestBase requestBase, List<Integer> expectedCodes) {
    if (apikey != null) {
      requestBase.setHeader(Constants.Gateway.CONTINUUITY_API_KEY, apikey);
    }
    if (accessToken != null) {
      requestBase.setHeader("Authorization", "Bearer " + accessToken);
    }
    try {
      HttpResponse response = client.execute(requestBase);
      // if expectedCodes is null, just check that we have OK status code
      if (expectedCodes == null) {
        if (!checkHttpStatus(response, HttpStatus.SC_OK)) {
          return null;
        }
      } else {
        if (!checkHttpStatus(response, expectedCodes)) {
          return null;
        }
      }
      return response;
    } catch (IOException e) {
      System.err.println("Error sending HTTP request: " + e.getMessage());
      return null;
    }
  }

  protected Long parseNumericArg(CommandLine line, String option) {
    if (line.hasOption(option)) {
      try {
        return Long.valueOf(line.getOptionValue(option));
      } catch (NumberFormatException e) {
        usage(option + " must have an integer argument");
      }
    }
    return null;
  }

  /**
   * Print an error message followed by the usage statement.
   *
   * @param errorMessage the error message
   */
  protected void usage(String errorMessage) {
    if (errorMessage != null) {
      System.err.println("Error: " + errorMessage);
    }
    printUsage(true);
  }

  /**
   * Prints the usage message to the PrintStream indicated by the error parameter
   *
   * @param error Indicates which stream to print to. If true, throws UsageException.
   */
  protected void printUsage(boolean error) {
    // print the positional args, if any, from child class
    printUsageTop(error);
    PrintWriter pw = error ? new PrintWriter(System.err) : new PrintWriter(System.out);
    pw.println("Options:\n");
    HelpFormatter formatter = new HelpFormatter();
    formatter.printOptions(pw, 100, options, 0, 10);
    pw.flush();
    pw.close();
    if (error) {
      throw new UsageException();
    }
  }

  /**
   * Reads the access token from the access token file. Returns null if the read fails
   * @return the access token from the access token file. Null if the read fails.
   */
  protected String readTokenFile() {
    if (tokenFile != null) {
      try {
        return Files.toString(new File(tokenFile), Charsets.UTF_8).replaceAll("(\r|\n)", "");
      } catch (FileNotFoundException e) {
        usage("Could not find access token file: " + tokenFile);
      } catch (IOException e) {
        usage("Could not read access token file: " + tokenFile);
      }
    }
    return null;
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

  /**
   * Prints the error response from the connection
   * @param errorStream the stream to read the response from
   */
  protected void readUnauthorizedError(InputStream errorStream) {
    PrintStream out = verbose ? System.out : System.err;
    out.println(HttpStatus.SC_UNAUTHORIZED + " Unauthorized");
    if (accessToken == null) {
      out.println("No access token provided");
      return;
    }
    try {
      Reader reader = new InputStreamReader(errorStream);
      String responseError = GSON.fromJson(reader, ErrorMessage.class).getErrorDescription();
      if (responseError != null && !responseError.isEmpty()) {
        out.println(responseError);
      }
    } catch (Exception e) {
      out.println("Unknown unauthorized error");
    }
  }

  /**
   * Check whether the Http return code is as expected. If not, print the error
   * message and return false. Otherwise, if verbose is on, print the response
   * status line.
   *
   * @param response the HTTP response
   * @param expected the expected HTTP status code
   * @return whether the response is as expected
   */
  protected boolean checkHttpStatus(HttpResponse response, int expected) {
    return checkHttpStatus(response, Collections.singletonList(expected));
  }

  /**
   * Check whether the Http return code is as expected. If not, print the
   * status message and return false. Otherwise, if verbose is on, print the
   * response status line.
   *
   * @param response the HTTP response
   * @param expected the list of expected HTTP status codes
   * @return whether the response is as expected
   */
  protected boolean checkHttpStatus(HttpResponse response, List<Integer> expected) {
    try {
      return checkHttpStatus(response.getStatusLine().getStatusCode(), response.getStatusLine().toString(),
                             response.getEntity().getContent(), expected);
    } catch (Exception e) {
      // error stream cannot be received
      return checkHttpStatus(response.getStatusLine().getStatusCode(), response.getStatusLine().toString(),
                             null, expected);
    }
  }

  /**
   * Checks that an http request was executed properly. Compares statusCode to the expected codes. If the status code
   * is not expected and is unauthorized, then an appropriate message is given.
   * @param statusCode the status code of the http request
   * @param statusLine the status line which is printed
   * @param errorStream the error stream for handling unauthorized requests
   * @param expected the expeced status codes
   * @return
   */
  protected boolean checkHttpStatus(int statusCode, String statusLine,
                                    InputStream errorStream,
                                    List<Integer> expected) {
    if (!expected.contains(statusCode)) {
      PrintStream out = verbose ? System.out : System.err;
      if (statusCode == HttpStatus.SC_UNAUTHORIZED) {
        if (errorStream != null) {
          readUnauthorizedError(errorStream);
          return false;
        }
      }
      // other errors
      out.println(statusLine);
      return false;
    }
    if (verbose) {
      System.out.println(statusLine);
    }
    return true;
  }
}
