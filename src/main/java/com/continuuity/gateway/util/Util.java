package com.continuuity.gateway.util;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.gateway.Constants;
import org.apache.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class Util {

  private static final Logger LOG = LoggerFactory
      .getLogger(Util.class);

  /**
   * This methods inspects the searches the gateway configuration for
   * a connector that has (a subclass of) the given class. This is
   * useful for finding the connector that implements a specific
   * protocol, for instance, for Flume we would search for the base
   * class FlumeCollector.
   * This method will only succeed if there is a single connector
   * configured for the given class (if there are multiple matches,
   * then the result would be ambiguous).
   *
   * @param config             The gateway configuration
   * @param connectorBaseClass The class of base class of the connector to be found
   * @return the name of connector if successful, otherwise null.
   */
  public static String findConnector(CConfiguration config, Class connectorBaseClass) {

    List<String> connectorNames = new LinkedList<String>();

    // Retrieve the list of connectors in the gateway
    Collection<String> allConnectorNames = config.
        getStringCollection(Constants.CONFIG_CONNECTORS);

    // For each Connector
    for (String connectorName : allConnectorNames) {
      // Retrieve the connector's Class name
      String connectorClassName = config.get(
          Constants.buildConnectorPropertyName(connectorName,
              Constants.CONFIG_CLASSNAME));
      // no class name configured? skip!
      if (connectorClassName == null) {
        LOG.warn("No class configured for connector '" + connectorName + "'.");
        continue;
      }
      try {
        // test whether this connector is a subclass of the desired connector -> hit!
        Class connectorClass = Class.forName(connectorClassName);
        if (testClass(connectorBaseClass, connectorClass)) {
          LOG.debug("Found connector '" + connectorName + "' of type " + connectorClassName);
          connectorNames.add(connectorName);
        }
        // class cannot be found? skip!
      } catch (ClassNotFoundException e) {
        LOG.warn("Configured class " + connectorClassName +
            " for connector '" + connectorName + "' not found.");
      }
    }
    // make sure there is exactly one flume collector
    if (connectorNames.size() == 0) {
      LOG.error("No connector of type " + connectorBaseClass.getName() + " found in configuration.");
      return null;
    } else if (connectorNames.size() > 1) {
      LOG.error("Multiple connectors of type " + connectorBaseClass.getName() + " found: " + connectorNames);
      return null;
    }
    return connectorNames.iterator().next();
  }

  /**
   * This is a helper to check whether a connector is a subclass of the
   * desired base class. It exists solely to suppress the unchecked warning
   * for isAssignableFrom that I can't figure out how to write correctly,
   * and because Java seems to ignore SuppressWarnings in the middle of
   * method, I don't want to suppress warnings for the whole method of
   * findConnector().
   *
   * @param base  The base class
   * @param clazz The connector class
   * @return whether the connector is a subclass of the base
   */
  @SuppressWarnings("unchecked")
  static boolean testClass(Class base, Class clazz) {
    return base.isAssignableFrom(clazz);
  }

  /**
   * Read the contents of an Http response
   * @param response The Http response
   * @return the contents as a byte array
   */
  static public byte[] readHttpResponse(HttpResponse response) {
    byte[] binary;
    try {
      if (response.getEntity() == null) {
        LOG.error("Cannot read from HTTP response because it has no content.");
        return null;
      }
      int length = (int) response.getEntity().getContentLength();
      InputStream content = response.getEntity().getContent();
      binary = new byte[length];
      int offset = 0;
      while (length > 0) { // must iterate because input stream is not guaranteed to return all at once
        int bytesRead = content.read(binary, offset, length);
        offset += bytesRead;
        length -= bytesRead;
      }
      return binary;
    } catch (IOException e) {
      System.err.println("Cannot read from HTTP response: " + e.getMessage());
      return null;
    }

  }

  /**
   * Read the contents of a binary file into a byte array
   *
   * @param filename The name of the file
   * @return the content of the file if successful, otherwise null
   */
  static public byte[] readBinaryFile(String filename) {
    File file = new File(filename);
    if (!file.isFile()) {
      System.err.println("'" + filename + "' is not a regular file.");
      return null;
    }
    int bytesToRead = (int) file.length();
    byte[] bytes = new byte[bytesToRead];
    int offset = 0;
    try {
      FileInputStream input = new FileInputStream(filename);
      while (bytesToRead > 0) {
        int bytesRead = input.read(bytes, offset, bytesToRead);
        bytesToRead -= bytesRead;
        offset += bytesRead;
      }
      input.close();
      return bytes;
    } catch (FileNotFoundException e) {
      LOG.error("File '" + filename + "' cannot be opened: " + e.getMessage());
    } catch (IOException e) {
      LOG.error("Error reading from file '" + filename + "': " + e.getMessage());
    }
    return bytes;
  }

  /**
   * Convert a hexadecimal string into a byte array
   *
   * @param hex The string to convert
   * @return the byte array value of the String
   * @throws NumberFormatException if the string is ill-formed
   */
  static public byte[] hexValue(String hex) {
    // verify the length of the string
    if (hex.length() % 2 != 0) {
      throw new NumberFormatException("Hex string must have even length.");
    }
    byte[] bytes = new byte[hex.length() / 2];
    for (int i = 0; i < bytes.length; ++i) {
      byte hi = hexValue(hex.charAt(2 * i));
      byte lo = hexValue(hex.charAt(2 * i + 1));
      bytes[i] = (byte) (((hi << 4) & 0xF0) | lo);
    }
    return bytes;
  }

  /**
   * Convert a hexadecimal character into a byte
   *
   * @param hex The character to convert
   * @return the byte value of the character
   * @throws NumberFormatException if the character is not hexadecimal
   */
  static public byte hexValue(char hex) {
    if (hex >= '0' && hex <= '9')
      return (byte) (hex - '0');
    else if (hex >= 'a' && hex <= 'f')
      return (byte) (hex - 'a' + 10);
    else if (hex >= 'A' && hex <= 'F')
      return (byte) (hex - 'A' + 10);
    else
      throw new NumberFormatException("'" + hex + "' is not a hexadecimal character.");
  }

  /**
   * Convert a byte array into its hex string representation
   *
   * @param bytes the byte array to convert
   * @return A hex string representing the bytes
   */
  static public String toHex(byte[] bytes) {
    StringBuilder builder = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      try {
        builder.append(Character.forDigit(b >> 4 & 0x0F, 16));
        builder.append(Character.forDigit(b & 0x0F, 16));
      } catch (IllegalArgumentException e) {
        // odd, that should never happen
        e.printStackTrace();
      }
    }
    return builder.toString();
  }

  /**
   * decode an URL-encoded string into bytes
   */
  public static byte[] urlDecode(String str) {
    try { // we use a base encoding that accepts all byte values
      return URLDecoder.decode(str, "ISO8859_1").getBytes("ISO8859_1");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace(); return null; // cant' happen with ISO8859_1 = Latin1
    }
  }
  /**
   * URL-encode a binary string
   */
  public static String urlEncode(byte[] binary) {
    if (binary == null) return null;
    try { // we use a base encoding that accepts all byte values
      return URLEncoder.encode(new String(binary, "ISO8859_1"), "ISO8859_1");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace(); return null; // this cannot happen with ISO8859_1 = Latin1
    }
  }

}