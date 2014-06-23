package com.continuuity.security.tools;

import com.continuuity.common.utils.UsageException;
import com.continuuity.security.server.ExternalAuthenticationServer;
import com.google.common.io.ByteStreams;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.Console;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

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

  private boolean help = false;

  private String host;
  private int port = 10000;

  private String username;
  private String password;
  private String filePath;

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
    out.println("  --host <name>           To specify the host of gateway");
    out.println("  --username <user>       To specify the user to login as");
    out.println("  --password <password>   To specify the user password");
    out.println("  --file <path>           To specify the access token file");
    out.println("  --ssl                   To use SSL");
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
      } else if ("--file".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        filePath = args[pos];
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
    // Default values for host and port.
    if (host == null) {
      host = "localhost";
    }

    if (filePath == null) {
      usage("Specify --file to save to file");
    }

    if (username == null) {
      username = System.getProperty("user.name");
      if (username == null) {
        usage("Specify --username to login as a user.");
      }
    }
    System.out.println(String.format("Authenticating as: %s", username));

    if (password == null) {
      Console console = System.console();
      password = String.valueOf(console.readPassword("Password: "));
    }

  }

  private String getAuthenticationServerAddress() throws IOException {
    HttpClient client = new DefaultHttpClient();
    HttpGet get = new HttpGet(String.format("http://%s:%d", host, port));
    HttpResponse response = client.execute(get);

    if (response.getStatusLine().getStatusCode() == 200) {
      System.out.println("Security is not enabled for Reactor. No Access Token may be acquired");
      System.exit(0);
    }
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteStreams.copy(response.getEntity().getContent(), bos);
    String responseBody = bos.toString("UTF-8");
    bos.close();
    JsonParser parser = new JsonParser();
    JsonObject responseJson = (JsonObject) parser.parse(responseBody);
    JsonArray addresses = responseJson.get("auth_uri").getAsJsonArray();
    ArrayList<String> list = new ArrayList<String>();
    for(JsonElement e : addresses){
      list.add(e.getAsString());
    }
    return list.get(new Random().nextInt(list.size()));
  }

  public String execute0(String[] args) {
    validateArguments(args);
    if (help) {
      return "";
    }

    String baseUrl;
    try {
      baseUrl = getAuthenticationServerAddress();
    } catch (IOException e) {
      System.err.println("Could not find Authentication service to connect to.");
      return null;
    }

    System.out.println(String.format("Authentication server address is: %s", baseUrl));

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
    auth = auth.replaceAll("(\r|\n)", "");
    get.addHeader("Authorization", String.format("Basic %s", auth));
    try {
      response = client.execute(get);
    } catch (IOException e) {
      System.err.println("Error sending HTTP request: " + e.getMessage());
      return null;
    }
    if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
      System.out.println("Authentication failed. Please ensure that the username and password provided are correct.");
      return null;
    } else {
      try {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ByteStreams.copy(response.getEntity().getContent(), bos);
        String responseBody = bos.toString("UTF-8");
        bos.close();
        JsonParser parser = new JsonParser();
        JsonObject responseJson = (JsonObject) parser.parse(responseBody);
        String token = responseJson.get(ExternalAuthenticationServer.ResponseFields.ACCESS_TOKEN).getAsString();

        PrintWriter writer = new PrintWriter(filePath, "UTF-8");
        writer.write(token);
        writer.close();
        System.out.println("Access Token saved to file access_token");
      } catch (Exception e) {
        System.err.println("Could not parse response contents.");
        e.printStackTrace(System.err);
        return null;
      }
    }
    client.getConnectionManager().shutdown();
    return "OK.";
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
    AccessTokenClient accessTokenClient = new AccessTokenClient();
    String value = accessTokenClient.execute(args);
    if (value == null) {
      System.exit(1);
    }
  }
}
