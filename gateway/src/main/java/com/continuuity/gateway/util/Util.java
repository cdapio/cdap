package com.continuuity.gateway.util;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.gateway.Constants;
import com.google.common.base.Charsets;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Utility class containing helpers.
 */
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
   * @param connectorBaseClass The class of base class of the connector to
   *                           be found
   * @return the name of connector if successful, otherwise null.
   */
  public static String findConnector(CConfiguration config,
                                     Class connectorBaseClass) {

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
        // test whether this connector is a subclass of the desired connector
        Class connectorClass = Class.forName(connectorClassName);
        if (testClass(connectorBaseClass, connectorClass)) {
          LOG.debug("Found connector '" + connectorName +
                      "' of type " + connectorClassName);
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
      LOG.error("No connector of type " + connectorBaseClass.getName() +
                  " found in configuration.");
      return null;
    } else if (connectorNames.size() > 1) {
      LOG.error("Multiple connectors of type " + connectorBaseClass.getName() +
                  " found: " + connectorNames);
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
   * Retrieves the http config of an http-based connector from the gateway
   * configuration. If no name is passed in, tries to figures out the
   * name by scanning through the configuration. Then it uses the
   * obtained Http config to create the base url for requests.
   *
   * @param config        The gateway configuration
   * @param connectorName The name of the connector, optional
   * @param hostname      The hostname to use for the url, optional. Note that
   *                      the connector's HttpConfig does not have a hostname
   *                      because it specifies a local inet address, hence it
   *                      would use 0.0.0.0 or localhost. This parameter helps
   *                      to correct the hostname portion of the returned url.
   * @return The base url if found, or null otherwise.
   */
  public static String findBaseUrl(CConfiguration config, Class connectorClass,
                                   String connectorName, String hostname,
                                   int port, boolean ssl) {

    if (connectorName == null) {
      // find the name of the connector
      connectorName = Util.findConnector(config, connectorClass);
      if (connectorName == null) {
        return null;
      } else {
        LOG.info("Reading configuration for connector '" +
                   connectorName + "'.");
      }
    }
    // get the collector's http config
    HttpConfig httpConfig;
    try {
      httpConfig = HttpConfig.configure(connectorName, config, null);
    } catch (Exception e) {
      LOG.error("Exception reading Http configuration for connector '"
                  + connectorName + "': " + e.getMessage());
      return null;
    }
    if (port > 0) {
      httpConfig.setPort(port);
    }
    httpConfig.setSsl(ssl);
    return httpConfig.getBaseUrl(hostname);
  }

  /**
   * Read the contents of an Http response.
   *
   * @param response The Http response
   * @return the contents as a byte array
   */
  public static byte[] readHttpResponse(HttpResponse response) {
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
      while (length > 0) {
        // must iterate because input stream does not always return all at once
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
   * Read the contents of a binary file into a byte array.
   *
   * @param filename The name of the file
   * @return the content of the file if successful, otherwise null
   */
  public static byte[] readBinaryFile(String filename) {
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
      LOG.error(
        "Error reading from file '" + filename + "': " + e.getMessage());
    }
    return bytes;
  }

  /**
   * Convert a hexadecimal string into a byte array.
   *
   * @param hex The string to convert
   * @return the byte array value of the String
   * @throws NumberFormatException if the string is ill-formed
   */
  public static byte[] hexValue(String hex) {
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
   * Convert a hexadecimal character into a byte.
   *
   * @param hex The character to convert
   * @return the byte value of the character
   * @throws NumberFormatException if the character is not hexadecimal
   */
  public static byte hexValue(char hex) {
    if (hex >= '0' && hex <= '9') {
      return (byte) (hex - '0');
    } else if (hex >= 'a' && hex <= 'f') {
      return (byte) (hex - 'a' + 10);
    } else if (hex >= 'A' && hex <= 'F') {
      return (byte) (hex - 'A' + 10);
    } else {
      throw new NumberFormatException(
        "'" + hex + "' is not a hexadecimal character.");
    }
  }

  /**
   * Convert a byte array into its hex string representation.
   *
   * @param bytes the byte array to convert
   * @return A hex string representing the bytes
   */
  public static String toHex(byte[] bytes) {
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
   * decode an URL-encoded string into bytes.
   */
  public static byte[] urlDecode(String str) {
    try { // we use a base encoding that accepts all byte values
      return URLDecoder.decode(str, "ISO8859_1").getBytes("ISO8859_1");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
      return null; // cant' happen with ISO8859_1 = Latin1
    }
  }

  /**
   * URL-encode a binary string.
   */
  public static String urlEncode(byte[] binary) {
    if (binary == null) {
      return null;
    }
    try { // we use a base encoding that accepts all byte values
      return URLEncoder.encode(new String(binary, "ISO8859_1"), "ISO8859_1");
    } catch (UnsupportedEncodingException e) {
      // this cannot happen with ISO8859_1 = Latin1
      e.printStackTrace();
      return null;

    }
  }

  /**
   * Encode a byte string as a String.
   *
   * @param bytes    the byte string to encode
   * @param encoding the encoding to use - "url" for URL-encoded, "hex" for
   *                 hexadecimal, or any other valid encoding name
   * @return the encoded String
   */
  public static String encode(byte[] bytes, String encoding) {
    if (bytes == null) {
      return null;
    } else if (encoding == null) {
      return urlEncode(bytes);
    } else if ("url".equals(encoding)) {
      return urlEncode(bytes);
    } else if ("hex".equals(encoding)) {
      return toHex(bytes);
    } else {
      try {
        return new String(bytes, encoding);
      } catch (UnsupportedEncodingException e) {
        return urlEncode(bytes);
      }
    }
  }

  /**
   * Convert a long value into a big-endian byte array.
   *
   * @param value the value to convert
   * @return the bytes of the value
   */
  public static byte[] longToBytes(long value) {
    byte[] bytes = new byte[8];
    for (int i = 7; i >= 0; i--) {
      bytes[i] = (byte) (value & 0xff);
      value = value >> 8;
    }
    return bytes;
  }

  /**
   * Convert a big-endian byte-array into a long. This is intended to be used
   * with 8-byte arrays, but it does not check the length of the array. For
   * arrays of less than 8 bytes, this will produce the same value as if the
   * array was left-padded with zeros. For arrays longer than 8 bytes, only
   * the last 8 bytes are used.
   *
   * @param bytes the byte array to convert
   * @return the long value of the byte array
   */
  public static long bytesToLong(byte[] bytes) {
    long value = 0;
    for (byte aByte : bytes) {
      value = (value << 8) | (((long) aByte) & 0xff);
    }
    return value;
  }

  //-----------------------------------------------------------------------------------
  // the following 3 belong together: encodeBinary, decodeBinary, supportedEncoding
  //-----------------------------------------------------------------------------------

  public static String encodeBinary(byte[] binary, String encoding) {
    return encodeBinary(binary, encoding, false);
  }

  public static String encodeBinary(byte[] binary, String encoding, boolean counter) {
    if (counter && Bytes.SIZEOF_LONG == binary.length) {
      return Long.toString(Bytes.toLong(binary));
    }
    if (encoding == null) {
      return new String(binary, Charsets.US_ASCII);
    }
    if ("hex".equals(encoding)) {
      return toHex(binary);
    }
    if ("base64".equals(encoding)) {
      return Base64.encodeBase64URLSafeString(binary);
    }
    if ("url".equals(encoding)) {
      try {
        // URLEncoder does not take byte[], so we convert it into a String with the same code points using
        // ISO-8859-1. This string only contains code points 0-255. Then we URL-encode that string using
        // ISO-8859-1 again. This way, every byte ends up as the exact same, %-escaped byte, in the URL string
        return URLEncoder.encode(new String(binary, Charsets.ISO_8859_1), Charsets.UTF_8.name());
      } catch (UnsupportedEncodingException e) {
        // can't happen
        throw new RuntimeException("Charsets.ISO_8859_1 is unsupported? Something is wrong with the JVM", e);
      }
    }
    // this can never happen because we only call it with null, hex, base64, or url
    throw new RuntimeException("Unexpected encoding: " + encoding + " Only hex, base64 and url are supported.");
  }

  public static byte[] decodeBinary(String str, String encoding) throws NumberFormatException {
    return decodeBinary(str, encoding, false);
  }

  public static byte[] decodeBinary(String str, String encoding, boolean counter) throws NumberFormatException {
    if (counter) {
      return Bytes.toBytes(Long.parseLong(str));
    }
    if (encoding == null) {
      return str.getBytes(Charsets.US_ASCII);
    }
    if ("hex".equals(encoding)) {
      return hexValue(str);
    }
    if ("base64".equals(encoding)) {
      return Base64.decodeBase64(str);
    }
    if ("url".equals(encoding)) {
      try {
        // URLDecoder does not produce byte[], so we decode to a String using ISO-8859. Note that the URL
        // decoder produces chars between 0-255, and thus each %-escaped byet in the URL results in
        // exactly one char in the string. That can be safely converted 1:1 into byte[] using getBuytes().
        return URLDecoder.decode(str, Charsets.ISO_8859_1.name()).getBytes(Charsets.ISO_8859_1);
      } catch (UnsupportedEncodingException e) {
        // can't happen
        throw new RuntimeException("Charsets.ISO_8859_1 is unsupported? Something is wrong with the JVM", e);
      }
    }
    // this can never happen because we only call it with null, hex, base64, or url
    throw new RuntimeException("Unexpected encoding: " + encoding + " Only hex, base64 and url are supported.");
  }

  public static boolean supportedEncoding(String enc) {
    return "hex".equals(enc) || "url".equals(enc) || "base64".equals(enc);
  }

}
