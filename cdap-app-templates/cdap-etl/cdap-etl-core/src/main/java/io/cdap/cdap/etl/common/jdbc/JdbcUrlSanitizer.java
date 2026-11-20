/*
 * Copyright © 2026 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.etl.common.jdbc;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Validates JDBC connection URLs and connection properties to prevent dangerous driver-specific
 * properties, arbitrary file reads, JNDI lookups, deserialization, and script execution.
 */
public final class JdbcUrlSanitizer {

  private static final int MAX_DECODE_PASSES = 5;
  private static final Pattern COMMENT_PATTERN = Pattern.compile("/\\*.*?\\*/", Pattern.DOTALL);
  private static final Pattern PAREN_CONTENT_PATTERN = Pattern.compile("\\(([^()]+)\\)");
  private static final Pattern VALID_PCT_HEX_PATTERN = Pattern.compile("%([0-9a-fA-F]{2})");

  private static final Set<String> ALLOWED_CLOUDSQL_SOCKET_FACTORIES =
    Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      "com.google.cloud.sql.mysql.SocketFactory",
      "com.google.cloud.sql.postgres.SocketFactory"
    )));

  private static final Set<String> BLOCKED_URL_PREFIXES =
    Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      "jdbc:h2:",
      "jdbc:sqlite:",
      "jdbc:derby:",
      "jdbc:modeshape:",
      "jdbc:jndi:",
      "jdbc:ldap:",
      "jdbc:ldaps:",
      "jdbc:rmi:",
      "jdbc:rowset:",
      "jdbc:hsqldb:file:",
      "jdbc:hsqldb:res:",
      "jdbc:tc:",
      "jdbc:oracle:oci"
    )));

  private static final Set<String> BLOCKED_URL_SCHEMES =
    Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      "ldap://",
      "ldaps://",
      "rmi://",
      "dns://",
      "iiop://",
      "corbaname:",
      "jndi:"
    )));

  private static final Set<String> BLOCKED_EXACT_KEYS =
    Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      // MySQL / MariaDB local file read, deserialization, and class instantiation
      "allowloadlocalinfile",
      "allowurlinlocalinfile",
      "allowloadlocalinfileinpath",
      "autodeserialize",
      "queryinterceptors",
      "statementinterceptors",
      "connectionlifecycleinterceptors",
      "exceptioninterceptors",
      "authenticationplugins",
      "defaultauthenticationplugin",
      "disabledauthenticationplugins",
      "clientinfoprovider",
      "propertiestransform",
      "connectionattributes",
      "loadbalancestrategy",
      "loadbalanceexceptionchecker",
      "loadbalanceautocommitstatementregex",
      "ha.loadbalancestrategy",
      "replicationconnectiongroup",
      "logger",
      "loggerclass",
      "loggerclassname",
      "loggerlevel",
      "loggerfile",
      "profilesql",
      "profilereventhandler",
      "parseinfocachefactory",
      "resultsetscannerregex",
      "customsocketfactory",
      "serverconfigcachefactory",
      "localinfileinputstream",
      // PostgreSQL socket / SSL / log / auth class instantiation and file write
      "socketfactory",
      "socketfactoryarg",
      "socketfactoryconstructorarg",
      "sslfactory",
      "sslfactoryarg",
      "sslpasswordcallback",
      "sslhostnameverifier",
      "authenticationpluginclassname",
      "gsslib",
      "service",
      // H2 / Derby / HSQLDB script execution and trace output
      "init",
      "script",
      "runscript",
      "trace_level_system_out",
      "trace_level_file",
      "create",
      "shutdown",
      // DB2 / Oracle / MSSQL / Teradata / Databricks dangerous properties
      "clientrerouteserverlistjndiname",
      "enableclientaffinitieslist",
      "pluginname",
      "securitymechanism",
      "tracefile",
      "tracefileappend",
      "tracedirectory",
      "logdirectory",
      "logfilepath",
      "servercert",
      "ttc_type_rep",
      "oracle.jdbc.drivernameattribute",
      "javax.naming.provider.url",
      "java.naming.provider.url",
      "java.naming.factory.initial",
      "socketfactoryclass",
      "socketfactoryconstructor",
      "krbjaasfile",
      "jaasconfigurationname",
      "loginconfigfile"
    )));

  private static final Set<String> BLOCKED_KEY_SUBSTRINGS =
    Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      "localinfile",
      "autodeserialize",
      "interceptor",
      "propertiestransform",
      "clientinfoprovider",
      "authenticationplugin",
      "loadbalancestrategy",
      "loadbalanceexceptionchecker",
      "profilereventhandler",
      "parseinfocachefactory",
      "serverconfigcachefactory",
      "socketfactory",
      "sslfactory",
      "sslpasswordcallback",
      "sslhostnameverifier",
      "loggerfile",
      "loggerlevel",
      "loggerclass",
      "tracefile",
      "tracedirectory",
      "trace_level",
      "jndi",
      "runscript",
      "jaasfile"
    )));

  private static final Set<String> SENSITIVE_PATH_TOKENS =
    Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      "/var/run/secrets",
      "/run/secrets",
      "kubernetes.io/serviceaccount",
      "/proc/",
      "/sys/",
      "/etc/",
      "/root/",
      "/home/",
      "/tmp/",
      "/var/tmp",
      "/var/log",
      "/dev/",
      "../",
      "/..",
      "..\\",
      "c:/windows",
      "c:/users"
    )));

  private static final Set<String> FILE_OR_URL_BEARING_KEY_TOKENS =
    Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      "file",
      "path",
      "dir",
      "url",
      "store",
      "cert",
      "key",
      "init",
      "script",
      "config",
      "jaas",
      "krb",
      "log",
      "trace",
      "dump",
      "xml",
      "xsl",
      "stylesheet"
    )));

  private JdbcUrlSanitizer() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  /**
   * Validates a JDBC connection URL and returns the validated URL.
   *
   * @param url the JDBC connection URL to validate
   * @return the validated URL
   * @throws IllegalArgumentException if the URL is null, empty, malformed, or contains disallowed properties
   */
  public static String sanitizeUrl(String url) {
    validateUrl(url);
    return url;
  }

  /**
   * Validates a JDBC connection URL against blocked protocols, dangerous parameters, and bypass encodings.
   *
   * @param url the JDBC connection URL to validate
   * @throws IllegalArgumentException if the URL is null, empty, malformed, or contains disallowed properties
   */
  public static void validateUrl(String url) {
    if (url == null || url.trim().isEmpty()) {
      throw new IllegalArgumentException("JDBC connection URL must not be null or empty.");
    }
    rejectControlChars(url, "JDBC URL");
    String decodedUrl = decodeFully(url, "JDBC URL");
    String commentStrippedUrl = stripComments(decodedUrl);
    if (commentStrippedUrl.contains("/*") || commentStrippedUrl.contains("*/")) {
      throw new IllegalArgumentException("JDBC connection URL contains unclosed or malformed comment sequence.");
    }

    String trimmedUrl = commentStrippedUrl.trim();
    String lowerUrl = trimmedUrl.toLowerCase(Locale.ROOT);
    String compactLowerUrl = lowerUrl.replaceAll("\\s+", "");

    if (!lowerUrl.startsWith("jdbc:") || !compactLowerUrl.startsWith("jdbc:")) {
      throw new IllegalArgumentException("Invalid JDBC connection URL: must start with 'jdbc:'.");
    }

    for (String blockedPrefix : BLOCKED_URL_PREFIXES) {
      if (lowerUrl.startsWith(blockedPrefix) || compactLowerUrl.startsWith(blockedPrefix)) {
        throw new IllegalArgumentException(
          String.format("Disallowed JDBC subprotocol in connection URL: '%s'.", blockedPrefix));
      }
    }

    for (String blockedScheme : BLOCKED_URL_SCHEMES) {
      if (lowerUrl.contains(blockedScheme) || compactLowerUrl.contains(blockedScheme)) {
        throw new IllegalArgumentException(
          String.format("Disallowed scheme '%s' in JDBC connection URL.", blockedScheme));
      }
    }

    parseAndValidateUrlParameters(url);
    if (!decodedUrl.equals(url)) {
      parseAndValidateUrlParameters(decodedUrl);
    }
    if (!commentStrippedUrl.equals(decodedUrl)) {
      parseAndValidateUrlParameters(commentStrippedUrl);
    }
  }

  /**
   * Validates a JDBC {@link Properties} object (including any backing default properties).
   *
   * @param properties the JDBC connection properties, may be null
   * @throws IllegalArgumentException if any property key or value is disallowed
   */
  public static void validateProperties(@Nullable Properties properties) {
    if (properties == null) {
      return;
    }
    for (String name : properties.stringPropertyNames()) {
      checkKeyAndValue(name, properties.getProperty(name), false);
    }
    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      String key = entry.getKey() == null ? null : String.valueOf(entry.getKey());
      String value = entry.getValue() == null ? null : String.valueOf(entry.getValue());
      checkKeyAndValue(key, value, false);
    }
  }

  /**
   * Validates a map of JDBC connection arguments.
   *
   * @param arguments the JDBC connection arguments map, may be null
   * @throws IllegalArgumentException if any argument key or value is disallowed
   */
  public static void validateConnectionArgumentsMap(@Nullable Map<String, String> arguments) {
    if (arguments == null || arguments.isEmpty()) {
      return;
    }
    for (Map.Entry<String, String> entry : arguments.entrySet()) {
      checkKeyAndValue(entry.getKey(), entry.getValue(), false);
    }
  }

  /**
   * Validates a raw connection arguments string (e.g. {@code key1=val1;key2=val2}).
   *
   * @param connectionArguments the raw connection arguments string, may be null
   * @throws IllegalArgumentException if any argument key or value is disallowed
   */
  public static void validateConnectionArgumentsString(@Nullable String connectionArguments) {
    if (connectionArguments == null || connectionArguments.trim().isEmpty()) {
      return;
    }
    rejectControlChars(connectionArguments, "JDBC connection arguments");
    String[] tokens = connectionArguments.split("[;&\\r\\n]+");
    for (String token : tokens) {
      validateKeyValueToken(token, false, false);
    }
  }

  /**
   * Validates both a JDBC connection URL and a {@link Properties} instance.
   *
   * @param url        the JDBC connection URL
   * @param properties the JDBC connection properties, may be null
   * @throws IllegalArgumentException if the URL or properties contain disallowed settings
   */
  public static void validateJdbcConnection(String url, @Nullable Properties properties) {
    validateUrl(url);
    validateProperties(properties);
  }

  private static void parseAndValidateUrlParameters(String urlStr) {
    String parenScan = urlStr;
    Matcher parenMatcher = PAREN_CONTENT_PATTERN.matcher(parenScan);
    while (parenMatcher.find()) {
      String groupContent = parenMatcher.group(1);
      for (String token : groupContent.split("[,;&?#]+")) {
        validateKeyValueToken(token, true, true);
      }
      parenScan = parenMatcher.replaceFirst(" ");
      parenMatcher = PAREN_CONTENT_PATTERN.matcher(parenScan);
    }

    int qIdx = urlStr.indexOf('?');
    if (qIdx >= 0 && qIdx + 1 < urlStr.length()) {
      String queryPart = urlStr.substring(qIdx + 1);
      for (String token : queryPart.split("[&;#?]+")) {
        validateKeyValueToken(token, true, true);
      }
    }

    String[] semiParts = urlStr.split(";");
    String[] parenStrippedSemiParts = parenScan.split(";");
    for (int i = 0; i < semiParts.length; i++) {
      if (i == 0) {
        String firstPart = parenStrippedSemiParts.length > 0 ? parenStrippedSemiParts[0] : semiParts[0];
        int qInFirst = firstPart.indexOf('?');
        String beforeQuery = qInFirst >= 0 ? firstPart.substring(0, qInFirst) : firstPart;
        if (beforeQuery.contains("=")) {
          for (String token : beforeQuery.split("[&#]+")) {
            validateKeyValueToken(token, true, true);
          }
        }
      } else {
        for (String token : semiParts[i].split("[&#?]+")) {
          validateKeyValueToken(token, true, true);
        }
      }
    }

    String lower = stripComments(urlStr).trim().toLowerCase(Locale.ROOT);
    if (lower.startsWith("jdbc:teradata:")) {
      String basePart = urlStr;
      int firstQuestion = basePart.indexOf('?');
      if (firstQuestion >= 0) {
        basePart = basePart.substring(0, firstQuestion);
      }
      int firstSemi = basePart.indexOf(';');
      if (firstSemi >= 0) {
        basePart = basePart.substring(0, firstSemi);
      }
      int schemeSlashes = basePart.indexOf("//");
      int pathSlash = schemeSlashes >= 0 ? basePart.indexOf('/', schemeSlashes + 2) : basePart.indexOf('/');
      if (pathSlash >= 0 && pathSlash + 1 < basePart.length()) {
        String teradataParams = basePart.substring(pathSlash + 1);
        for (String token : teradataParams.split("[,/]+")) {
          validateKeyValueToken(token, true, true);
        }
      }
    }
  }

  private static void validateKeyValueToken(String token, boolean fromUrl, boolean stripKeyPrefixDelimiters) {
    if (token == null) {
      return;
    }
    String trimmed = token.trim();
    if (trimmed.isEmpty()) {
      return;
    }
    int eqIdx = trimmed.indexOf('=');
    if (eqIdx < 0) {
      return;
    }
    String rawKey = trimmed.substring(0, eqIdx).trim();
    String rawVal = trimmed.substring(eqIdx + 1).trim();
    if (stripKeyPrefixDelimiters) {
      int lastDelim = Math.max(
        Math.max(rawKey.lastIndexOf(':'), rawKey.lastIndexOf('/')),
        Math.max(rawKey.lastIndexOf('?'), rawKey.lastIndexOf('('))
      );
      if (lastDelim >= 0 && lastDelim + 1 < rawKey.length()) {
        rawKey = rawKey.substring(lastDelim + 1).trim();
      }
    }
    checkKeyAndValue(rawKey, rawVal, fromUrl);
  }

  private static void checkKeyAndValue(@Nullable String rawKey, @Nullable String rawValue, boolean fromUrl) {
    if (rawKey == null) {
      return;
    }
    rejectControlChars(rawKey, "JDBC property key");
    String decodedKey = decodeFully(rawKey, "JDBC property key");
    String normalizedKey = normalizeKey(decodedKey);
    if (normalizedKey.isEmpty()) {
      return;
    }

    String decodedValue = null;
    if (rawValue != null) {
      rejectControlChars(rawValue, "JDBC property value");
      if ("password".equals(normalizedKey) && !fromUrl) {
        decodedValue = decodeLenient(rawValue);
      } else {
        decodedValue = decodeFully(rawValue, "JDBC property value");
      }
    }

    if ("socketfactory".equals(normalizedKey)) {
      if (fromUrl && rawValue != null && decodedValue != null) {
        String trimmedRaw = rawValue.trim();
        String trimmedDecoded = stripComments(decodedValue).trim();
        if (ALLOWED_CLOUDSQL_SOCKET_FACTORIES.contains(trimmedRaw)
            && ALLOWED_CLOUDSQL_SOCKET_FACTORIES.contains(trimmedDecoded)) {
          return;
        }
      }
      throw new IllegalArgumentException(
        String.format("Disallowed JDBC connection property: '%s'.", decodedKey.trim()));
    }

    if (BLOCKED_EXACT_KEYS.contains(normalizedKey)) {
      throw new IllegalArgumentException(
        String.format("Disallowed JDBC connection property: '%s'.", decodedKey.trim()));
    }

    for (String blockedSubstring : BLOCKED_KEY_SUBSTRINGS) {
      if (normalizedKey.contains(blockedSubstring)) {
        throw new IllegalArgumentException(
          String.format("Disallowed JDBC connection property: '%s'.", decodedKey.trim()));
      }
    }

    if (decodedValue != null) {
      validatePropertyValue(normalizedKey, decodedKey, decodedValue);
    }
  }

  private static void validatePropertyValue(String normalizedKey, String displayKey, String decodedValue) {
    String strippedVal = stripBraces(stripComments(decodedValue).trim());
    String lowerVal = strippedVal.toLowerCase(Locale.ROOT);
    String compactLowerVal = lowerVal.replaceAll("\\s+", "");

    for (String blockedScheme : BLOCKED_URL_SCHEMES) {
      if (lowerVal.contains(blockedScheme) || compactLowerVal.contains(blockedScheme)) {
        throw new IllegalArgumentException(
          String.format("Disallowed scheme in value for JDBC property '%s'.", displayKey.trim()));
      }
    }

    if (compactLowerVal.startsWith("file:") || compactLowerVal.contains("=file:")
        || compactLowerVal.startsWith("jar:") || compactLowerVal.contains("=jar:")
        || compactLowerVal.contains("javascript:")
        || compactLowerVal.contains("runscript")
        || compactLowerVal.contains("createalias")) {
      throw new IllegalArgumentException(
        String.format("Disallowed value for JDBC property '%s'.", displayKey.trim()));
    }

    if (isSensitiveFilePath(lowerVal) || isSensitiveFilePath(compactLowerVal)) {
      throw new IllegalArgumentException(
        String.format("Disallowed sensitive file path in JDBC property '%s'.", displayKey.trim()));
    }

    if (isFileOrUrlBearingKey(normalizedKey)) {
      if (compactLowerVal.startsWith("http://") || compactLowerVal.startsWith("https://")
          || compactLowerVal.startsWith("ftp://")) {
        throw new IllegalArgumentException(
          String.format("Disallowed remote URL in JDBC property '%s'.", displayKey.trim()));
      }
    }
  }

  private static boolean isFileOrUrlBearingKey(String normalizedKey) {
    for (String token : FILE_OR_URL_BEARING_KEY_TOKENS) {
      if (normalizedKey.contains(token)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isSensitiveFilePath(String lowerVal) {
    String slashNormalized = lowerVal.replace('\\', '/');
    for (String sensitiveToken : SENSITIVE_PATH_TOKENS) {
      if (slashNormalized.contains(sensitiveToken)) {
        return true;
      }
    }
    return false;
  }

  private static String normalizeKey(String key) {
    String commentStripped = stripComments(key);
    if (commentStripped.contains("/*") || commentStripped.contains("*/")) {
      throw new IllegalArgumentException("JDBC property key contains unclosed or malformed comment sequence.");
    }
    StringBuilder sb = new StringBuilder(commentStripped.length());
    for (int i = 0; i < commentStripped.length(); i++) {
      char c = commentStripped.charAt(i);
      if (Character.isWhitespace(c) || c == '"' || c == '\'' || c == '`' || c == '\\' || c == '{' || c == '}') {
        continue;
      }
      sb.append(Character.toLowerCase(c));
    }
    return sb.toString();
  }

  private static String stripBraces(String value) {
    String current = value;
    while (current.length() >= 2 && current.startsWith("{") && current.endsWith("}")) {
      current = current.substring(1, current.length() - 1).trim();
    }
    return current;
  }

  private static String stripComments(String input) {
    String current = input;
    String previous;
    do {
      previous = current;
      current = COMMENT_PATTERN.matcher(current).replaceAll("");
    } while (!current.equals(previous));
    return current;
  }

  private static void rejectControlChars(String value, String context) {
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (c == '\0' || c == '\r' || c == '\n') {
        throw new IllegalArgumentException(
          String.format("Invalid control character in %s.", context));
      }
    }
  }

  private static String decodeFully(String input, String context) {
    String current = input;
    for (int pass = 0; pass < MAX_DECODE_PASSES; pass++) {
      if (!current.contains("%") && (pass > 0 || !current.contains("+"))) {
        return current;
      }
      String decoded;
      try {
        String prepped = pass == 0 ? current.replace("+", "%20") : current.replace("+", "%2B");
        decoded = URLDecoder.decode(prepped, StandardCharsets.UTF_8.name());
      } catch (IllegalArgumentException | UnsupportedEncodingException e) {
        throw new IllegalArgumentException(
          String.format("Malformed percent-encoding in %s.", context), e);
      }
      rejectControlChars(decoded, context);
      if (decoded.equals(current)) {
        return decoded;
      }
      current = decoded;
    }
    if (current.contains("%")) {
      throw new IllegalArgumentException(
        String.format("Excessive nested percent-encoding in %s.", context));
    }
    return current;
  }

  private static String decodeLenient(String input) {
    String current = input;
    for (int pass = 0; pass < MAX_DECODE_PASSES; pass++) {
      Matcher matcher = VALID_PCT_HEX_PATTERN.matcher(current);
      if (!matcher.find()) {
        return current;
      }
      StringBuffer sb = new StringBuffer();
      matcher.reset();
      while (matcher.find()) {
        int byteVal = Integer.parseInt(matcher.group(1), 16);
        char decodedChar = (char) byteVal;
        matcher.appendReplacement(sb, Matcher.quoteReplacement(String.valueOf(decodedChar)));
      }
      matcher.appendTail(sb);
      String next = sb.toString();
      rejectControlChars(next, "JDBC password property");
      if (next.equals(current)) {
        return next;
      }
      current = next;
    }
    return current;
  }
}
