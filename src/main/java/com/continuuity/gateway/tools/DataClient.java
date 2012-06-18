package com.continuuity.gateway.tools;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.gateway.accessor.RestAccessor;
import com.continuuity.gateway.util.Util;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

/**
 * This is a command line tool to interact with the key/vaue store of
 * data fabric. It can get a value by key, save a value for a key,
 * delete a key, or list all keys in the store.
 * <ul>
 * <li>It attempts to be smart and determine the URL of the REST
 * accessor auto-magically. If that fails, the user can give hints
 * via the --connector and --base arguments</li>
 * <li>The key can be read in binary form from a file, or provided
 * on the command line as a String in various encodings, as indicated
 * by the --hex, --url, and --encoding arguments</li>
 * <li>The value can be saved to a file in binary form, or printed
 * to the screen in the same encoding as the key.</li>
 * </ul>
 */
public class DataClient {

  /**
   * for debugging. should only be set to true in unit tests.
   * when true, program will print the stack trace after the usage.
   */
  public static boolean debug = false;

  boolean help = false;          // whether --help was there
  boolean verbose = false;       // for debug output
  String command = null;         // the command to run
  String baseUrl = null;         // the base url for HTTP requests
  String hostname = null;        // the hostname of the gateway
  String connector = null;       // the name of the rest accessor
  String key = null;             // the key to read/write/delete
  String value = null;           // the value to write
  String encoding = null;        // the encoding for --key and for display of the value
  boolean hexEncoded = false;    // whether --key and display of value use hexadecimal encoding
  boolean urlEncoded = false;    // whether --key and display of value use url encoding
  String keyFile = null;         // the file to read the key from
  String valueFile = null;       // the file to read/write the value from/to

  boolean keyNeeded;             // does the command require a key?
  boolean valueNeeded;           // does the command require a value?
  boolean outputNeeded;          // does the command require to write an output value?

  /**
   * Print the usage statement and return null (or empty string if this is not an error case).
   * See getValue() for an explanation of the return type.
   *
   * @param error indicates whether this was invoked as the result of an error
   * @throws IllegalArgumentException in case of error, an empty string in case of success
   */
  void usage(boolean error) {
    PrintStream out = (error ? System.err : System.out);
    String name = this.getClass().getSimpleName();
    out.println("Usage: ");
    out.println("  " + name + " read --key <string> [ <options> ]");
    out.println("  " + name + " write --key <string> --value value [ <options> ]");
    out.println("  " + name + " delete --key <string> [ <options> ]");
    out.println("  " + name + " list [ <options> ]");
    out.println("Additional options:");
    out.println("  --base <url>            To specify the base url to send to");
    out.println("  --host <name>           To specify the hostname to send to");
    out.println("  --connector <name>      To specify the name of the rest connector");
    out.println("  --key <string>          To specify the key");
    out.println("  --key-file <path>       To read the binary key from a file");
    out.println("  --value <string>        To specify the value");
    out.println("  --value-file <path>     To read/write the binary value from/to a file");
    out.println("  --hex                   To use hexadecimal encoding for key and value");
    out.println("  --ascii                 To use ASCII encoding for key and value");
    out.println("  --url                   To use URL encoding for key and value");
    out.println("  --encoding <name>       To use this encoding for key and value");
    out.println("  --verbose               To see more verbose output");
    out.println("  --help                  To print this message");
    if (error) {
      throw new IllegalArgumentException();
    }
  }

  /**
   * Print an error message followed by the usage statement
   * @param errorMessage the error message
   */
  void usage(String errorMessage) {
    if (errorMessage != null) System.err.println("Error: " + errorMessage);
    usage(true);
  }

  /**
   * Parse the command line arguments
   */
  void parseArguments(String[] args) {
    if (args.length == 0) usage(true);
    if ("--help".equals(args[0])) {
      usage(false);
      help = true;
      return;
    } else {
      command = args[0];
    }
    // go through all the arguments
    for (int pos = 1; pos < args.length; pos++) {
      String arg = args[pos];
      if ("--base".equals(arg)) {
        if (++pos >= args.length) usage(true);
        baseUrl = args[pos];
      } else if ("--host".equals(arg)) {
        if (++pos >= args.length) usage(true);
        hostname = args[pos];
      } else if ("--connector".equals(arg)) {
        if (++pos >= args.length) usage(true);
        connector = args[pos];
      } else if ("--key".equals(arg)) {
        if (++pos >= args.length) usage(true);
        key = args[pos];
      } else if ("--value".equals(arg)) {
        if (++pos >= args.length) usage(true);
        value = args[pos];
      } else if ("--key-file".equals(arg)) {
        if (++pos >= args.length) usage(true);
        keyFile = args[pos];
      } else if ("--value-file".equals(arg)) {
        if (++pos >= args.length) usage(true);
        valueFile = args[pos];
      } else if ("--encoding".equals(arg)) {
        if (++pos >= args.length) usage(true);
        encoding = args[pos];
      } else if ("--ascii".equals(arg)) {
        encoding = "ASCII";
      } else if ("--url".equals(arg)) {
        urlEncoded = true;
      } else if ("--hex".equals(arg)) {
        hexEncoded = true;
      } else if ("--verbose".equals(arg)) {
        verbose = true;
      } else if ("--help".equals(arg)) {
        help = true;
        usage(false);
        return;
      } else {  // unkown argument
        usage(true);
      }
    }
  }

  static List<String> supportedCommands = Arrays.asList("read", "write", "delete", "list");

  void validateArguments(String[] args) {
    // first parse command arguments
    parseArguments(args);
    if (help) return;
    // first validate the command
    if (!supportedCommands.contains(command)) usage("Unsupported command '" + command + "'.");
    // verify that either --key or --key-file is given, and same for --value and --value-file
    if (key != null && keyFile != null) usage("Only one of --key and --key-file may be specified");
    if (value != null && valueFile != null) usage("Only one of --value and --value-file may be specified");
    // verify that only one encoding was given
    int encodings = 0;
    keyNeeded = !command.equals("list");
    valueNeeded = command.equals("write");
    outputNeeded = command.equals("read") || command.equals("list");
    boolean needsEncoding = (keyNeeded && keyFile == null) || ((valueNeeded || outputNeeded) && valueFile == null);
    if (hexEncoded) ++encodings;
    if (urlEncoded) ++encodings;
    if (encoding != null) ++encodings;
    if (needsEncoding && encodings > 1) usage("Only one encoding can be specified.");
    if (!needsEncoding && encodings > 0) usage("Encoding may not be specified foe binary file.");
    // verify that only one hint is given for the URL
    if (hostname != null && baseUrl != null) usage("Only one of --host or --base may be specified.");
    if (connector != null && baseUrl != null) usage("Only one of --connector or --base may be specified.");
    // based on the command, ensure all arguments are there
    if ("read".equals(command)) {
      // read needs a key and possibly a file for the value
      if (key == null && keyFile == null) usage("A key must be specified - use either --key or --key-file.");
      if (value != null) usage("A value may not be specified for read.");
    }
    else if ("write".equals(command)) {
      // write needs a key and a value
      if (key == null && keyFile == null) usage("A key must be specified - use either --key or --key-file.");
      if (value == null && valueFile == null) usage("A value must be specified - use either --value or --value-file.");
    }
    else if ("delete".equals(command)) {
      // delete needs a key but never a value
      if (key == null && keyFile == null) usage("A key must be specified - use either --key or --key-file.");
      if (value != null || valueFile != null) usage("A value may not be specified for delete.");
    }
    else if ("list".equals(command)) {
      // list needs no key, but can have a file for the value
      if (key != null || keyFile != null) usage("A key may not be specified for list.");
      if (value != null) usage("A value may not be specified for list.");
    }
  }

  /**
   * read the key using arguments
   */
  byte[] readKeyOrValue(String what, String str, String file) {
    byte[] binary;
    // is the key in a file?
    if (file != null) {
      binary = Util.readBinaryFile(keyFile);
      if (binary == null) {
        System.err.println("Cannot read " + what + " from file " + file + ".");
        return null;
    } }
    // or is it in hexadecimal?
    else if (hexEncoded) {
      try {
        binary = Util.hexValue(str);
      } catch (NumberFormatException e) {
        System.err.println("Cannot parse '" + str + "' as hexadecimal: " + e.getMessage());
        return null;
    } }
    // or is it in URL encoding?
    else if (urlEncoded) {
      binary = Util.urlDecode(str);
    }
    // lastly, it can be in the given encoding
    else if (encoding != null) {
      try {
        binary = str.getBytes(encoding);
      } catch (UnsupportedEncodingException e) {
        System.err.println("Unsupported encoding " + encoding);
        return null;
    } }
    // nothing specified, use default encoding
    else
      binary = str.getBytes();

    return binary;
  }

  /*
   * return the resulting value to the use, following arguments
   */
  String writeValue(byte[] binaryValue) {
    // was a file specified to write to?
    if (valueFile != null) {
      try {
        FileOutputStream out = new FileOutputStream(valueFile);
        out.write(binaryValue);
        out.close();
        if (verbose) System.out.println(binaryValue.length + " bytes written to file " + valueFile + ".");
        return binaryValue.length + " bytes written to file";
      } catch (IOException e) {
        System.err.println("Error writing to file " + valueFile + ": " + e.getMessage());
        return null;
      } }
    // was hex encoding requested?
    if (hexEncoded) {
      value = Util.toHex(binaryValue);
    }
    // or was URl encoding specified?
    else if (urlEncoded) {
      try { // use a base encoding that supports all byte values
        value = URLEncoder.encode(new String(binaryValue, "ISO8859_1"), "ISO8859_1");
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace(); // this cannot happen
      } }
    // was a different encoding specified?
    else if (encoding != null) {
      try { // this may fail because encoding was user-specified
        value = new String(binaryValue, encoding);
      } catch (UnsupportedEncodingException e) {
        System.err.println("Unsupported encoding " + encoding);
        return null;
      }
    }
    // by default, assume the same encoding for the value as for the key
    else
      value = new String(binaryValue);

    if (verbose) System.out.println("Value[" + binaryValue.length + " bytes]: " + value);
    else System.out.println(value);
    return value;
  }

  /*
   * return the resulting value to the use, following arguments
   */
  String writeList(byte[] binaryResponse) {
    // was a file specified to write to?
    if (valueFile != null) {
      try {
        FileOutputStream out = new FileOutputStream(valueFile);
        out.write(binaryResponse);
        out.close();
        if (verbose) System.out.println(binaryResponse.length + " bytes written to file " + valueFile + ".");
        return binaryResponse.length + " bytes written to file";
      } catch (IOException e) {
        System.err.println("Error writing to file " + valueFile + ": " + e.getMessage());
        return null;
    } }
    else {
      try {
        System.out.write(binaryResponse);
        return binaryResponse.length + " bytes written to standard out";
      } catch (IOException e) {
        System.err.println("Error writing to standard out: " + e.getMessage());
        return null;
      }
    }
  }

  /**
   * This is actually the main method, but in order to make it testable, instead of exiting in case
   * of error it returns null, whereas in case of success it returns the retrieved value as shown
   * on the console.
   *
   * @param args   the command line arguments of the main method
   * @param config The configuration of the gateway
   * @return null in case of error, an string representing the retrieved value in case of success
   */
  public String execute0(String[] args, CConfiguration config) {
    // parse and validate arguments
    validateArguments(args);
    if (help) return "";

    // determine the base url for the GET request
    if (baseUrl == null)
      baseUrl = Util.findBaseUrl(config, RestAccessor.class, connector, hostname);
    if (baseUrl == null) {
      System.err.println("Can't figure out the URL to send to. Please use --base or --connector to specify.");
      return null;
    } else {
      if (verbose) System.out.println("Using base URL: " + baseUrl);
    }

    String urlEncodedKey = null;
    if (keyNeeded) {
      urlEncodedKey = Util.urlEncode(readKeyOrValue("key", key, keyFile));
      if (urlEncodedKey == null) return null;
    }
    byte[] binaryValue = null;
    if (valueNeeded) {
      binaryValue = readKeyOrValue("value", value, valueFile);
      if (binaryValue == null) return null;
    }

    // construct the full URL and verify its well-formedness
    String requestUrl = baseUrl + "default";
    if (keyNeeded) requestUrl += "/" + urlEncodedKey;
    if (verbose && !"list".equals(command)) System.out.println("Request URI is: " + requestUrl);
    URI uri;
    try {
      uri = URI.create(requestUrl);
    } catch (IllegalArgumentException e) {
      // this can only happen if the --host, or --base are not valid for a URL
      System.err.println("Invalid request URI '" + requestUrl
          + "'. Check the validity of --host or --base arguments.");
      return null;
    }

    // prepare for HTTP
    HttpClient client = new DefaultHttpClient();
    HttpResponse response;

    // the rest depends on the command
    if ("read".equals(command)) {
      try {
        response = client.execute(new HttpGet(uri));
        client.getConnectionManager().shutdown();
      } catch (IOException e) {
        System.err.println("Error sending HTTP request: " + e.getMessage());
        return null;
      }
      // show the HTTP status and verify it was successful
      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        System.err.println(response.getStatusLine());
        return null;
      } else {
        if (verbose) System.out.println(response.getStatusLine());
      }
      // read the binary value from the HTTP response
      binaryValue = Util.readHttpResponse(response);
      if (binaryValue == null) return null;
      // now make returned value available to user
      return writeValue(binaryValue);
    }
    else if ("write".equals(command)) {
      try {
        HttpPut put = new HttpPut(uri);
        put.setEntity(new ByteArrayEntity(binaryValue));
        response = client.execute(put);
        client.getConnectionManager().shutdown();
      } catch (IOException e) {
        System.err.println("Error sending HTTP request: " + e.getMessage());
        return null;
      }
      // show the HTTP status and verify it was successful
      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        System.err.println(response.getStatusLine());
        return null;
      } else {
        if (verbose) System.out.println(response.getStatusLine());
      }
      return "OK.";
    }
    else if ("delete".equals(command)) {
      try {
        response = client.execute(new HttpDelete(uri));
        client.getConnectionManager().shutdown();
      } catch (IOException e) {
        System.err.println("Error sending HTTP request: " + e.getMessage());
        return null;
      }
      // show the HTTP status and verify it was successful
      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        System.err.println(response.getStatusLine());
        return null;
      } else {
        if (verbose) System.out.println(response.getStatusLine());
      }
      return "OK.";
    }
    else if ("list".equals(command)) {
      // we have to massage the URL a little more
      //String enc = urlEncoded ? "url" : hexEncoded ? "hex" : encoding;
      requestUrl += "?q=list";
      if (urlEncoded) requestUrl += "&enc=url";
      else if (hexEncoded) requestUrl += "&enc=hex";
      else if (encoding != null) requestUrl += "&enc=" + encoding;
      else requestUrl += "&enc=" + Charset.defaultCharset().displayName();
      if (verbose) System.out.println("Request URI is: " + requestUrl);
      try {
        uri = URI.create(requestUrl);
      } catch (IllegalArgumentException e) {
        // this can only happen if the --host, or --base are not valid for a URL
        System.err.println("Invalid request URI '" + requestUrl
            + "'. Check the validity of --host or --base arguments.");
        return null;
      }
      // now execute this as a get
      try {
        response = client.execute(new HttpGet(uri));
        client.getConnectionManager().shutdown();
      } catch (IOException e) {
        System.err.println("Error sending HTTP request: " + e.getMessage());
        return null;
      }
      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        System.err.println(response.getStatusLine());
        return null;
      } else {
        if (verbose) System.out.println(response.getStatusLine());
      }
      // read the binary value from the HTTP response
      binaryValue = Util.readHttpResponse(response);
      if (binaryValue == null) return null;
      // now make returned value available to user
      return writeList(binaryValue);
    }
    return null;
  }

  public String execute(String[] args, CConfiguration config) {
    try {
      return execute0(args, config);
    } catch (IllegalArgumentException e) {
      if (debug) { // this is mainly for debugging the unit test
        System.err.println("Exception for arguments: " + Arrays.toString(args) + ". Exception: " + e);
        e.printStackTrace(System.err);
      }
    }
    return null;
  }

  /**
    * This is the main method. It delegates to getValue() in order to make
    * it possible to test the return value.
    */
  public static void main(String[] args) {
    // create a config and load the gateway properties
    CConfiguration config = CConfiguration.create();
    config.addResource("continuuity-gateway.xml");
    // create a data client and run it with the given arguments
    DataClient instance = new DataClient();
    String value = instance.execute(args, config);
    // exit with error in case fails
    if (value == null) System.exit(1);
  }
}

