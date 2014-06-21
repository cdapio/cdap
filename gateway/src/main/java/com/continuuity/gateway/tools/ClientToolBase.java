package com.continuuity.gateway.tools;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.UsageException;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.Collections;
import java.util.List;

/**
 * Base class for reactor tools
 */
public abstract class ClientToolBase {

  private String toolName;
  public static final Gson GSON = new Gson();

  private static final String HOST_OPTION = "host";
  private static final String PORT_OPTION = "port";
  private static final String HELP_OPTION = "help";
  private static final String VERBOSE_OPTION = "verbose";
  private static final String API_KEY_OPTION = "api-key";
  private static final String TOKEN_FILE_OPTION = "token-file";
  private static final String TOKEN_OPTION = "token";

  public Options options;

  public boolean help = false;
  public boolean verbose = false;
  public boolean forceNoSSL = false;
  public String apikey = null;          // the api key for authentication
  public String tokenFile = null;       // the file containing the access token only
  public String accessToken = null;     // the access token for secure connections
  public String hostname = null;
  public int port = -1;

  public String getToolName() {
    return this.toolName;
  }

  public ClientToolBase disallowSSL() {
    this.forceNoSSL = true;
    return this;
  }

  public void buildOptions() {
    options.addOption(null, HOST_OPTION, true, "To specify the reactor host");
    options.addOption(null, PORT_OPTION, true, "To specify the port to use. The default value is --port "
                      + Constants.Gateway.DEFAULT_PORT);
    options.addOption(null, HELP_OPTION, false, "To print this message");
    options.addOption(null, VERBOSE_OPTION, false, "Prints verbose output");
    options.addOption(null, API_KEY_OPTION, true, "To specify an API key for authentication");
    options.addOption(null, TOKEN_OPTION, true, "To specify the access token for secure reactor");
    options.addOption(null, TOKEN_FILE_OPTION, true, "To specify a path to the access token for secure reactor");
  }

  public boolean parseBasicArgs(CommandLine line) {
    if (line.hasOption(HELP_OPTION)) {
      printUsage(false);
      help = true;
      return false;
    }
    verbose = line.hasOption(VERBOSE_OPTION);
    hostname = line.hasOption(HOST_OPTION) ? line.getOptionValue(HOST_OPTION) : null;
    port = line.hasOption(PORT_OPTION) ? Integer.valueOf(line.getOptionValue(PORT_OPTION)) : -1;
    apikey = line.hasOption(API_KEY_OPTION) ? line.getOptionValue(API_KEY_OPTION) : null;
    accessToken = line.hasOption(TOKEN_OPTION) ? line.getOptionValue(TOKEN_OPTION).replaceAll("(\r|\n)", "") : null;
    tokenFile = line.hasOption(TOKEN_FILE_OPTION) ? line.getOptionValue(TOKEN_FILE_OPTION).replaceAll("(\r|\n)", "")
      : null;
    return true;
  }

  abstract boolean parseArguments(String[] args);
  abstract void validateArguments(String[] args);

  /**
   * Sends http requests with apikey and access token headers
   * and checks the status of the request afterwards.
   * @param requestBase The request to send. This method adds the apikey and access token headers
   *                    if they are valid.
   * @param expectedCodes The list of expected status codes from the request. If set to null,
   *                      this method checks that the status code is OK.
   * @return The HttpResponse if the request was successfully sent and the request status code
   * is one of expectedCodes or OK if expectedCodes is null. Otherwise, returns null.
   */
  HttpResponse sendHttpRequest(HttpRequestBase requestBase, List<Integer> expectedCodes) {
    if (apikey != null) {
      requestBase.setHeader(Constants.Gateway.CONTINUUITY_API_KEY, apikey);
    }
    if (accessToken != null) {
      requestBase.setHeader("Authorization", "Bearer " + accessToken);
    }
    HttpClient client = new DefaultHttpClient();
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
    } finally {
      client.getConnectionManager().shutdown();
    }
  }

  public Long parseNumericArg(CommandLine line, String option) {
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
  public void usage(String errorMessage) {
    if (errorMessage != null) {
      System.err.println("Error: " + errorMessage);
    }
    printUsage(true);
  }

  public void printUsage(boolean error) {
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
  public String readTokenFile() {
    if (tokenFile != null) {
      PrintStream out = verbose ? System.out : System.err;
      try {
        return Files.toString(new File(tokenFile), Charsets.UTF_8).replaceAll("(\r|\n)", "");
      } catch (FileNotFoundException e) {
        out.println("Could not find access token file: " + tokenFile + "\nNo access token will be used");
      } catch (IOException e) {
        out.println("Could not read access token file: " + tokenFile + "\nNo access token will be used");
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
  public void readUnauthorizedError(InputStream errorStream) {
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
  boolean checkHttpStatus(HttpResponse response, int expected) {
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
  boolean checkHttpStatus(HttpResponse response, List<Integer> expected) {
    try {
      return checkHttpStatus(response.getStatusLine().getStatusCode(), response.getStatusLine().toString(),
                             response.getEntity().getContent(), expected);
    } catch (IOException e) {
      System.err.println("Could not get error stream");
    }
    // in case the first part fails we just need to return if we expect the status code or not
    return expected.contains(response.getStatusLine().getStatusCode());
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
  public boolean checkHttpStatus(int statusCode, String statusLine, InputStream errorStream, List<Integer> expected) {
    if (!expected.contains(statusCode)) {
      PrintStream out = verbose ? System.out : System.err;
      if (statusCode == HttpStatus.SC_UNAUTHORIZED) {
        readUnauthorizedError(errorStream);
        return false;
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

  public ClientToolBase(String toolName) {
    this.toolName = toolName;
    options = new Options();
  }
}
