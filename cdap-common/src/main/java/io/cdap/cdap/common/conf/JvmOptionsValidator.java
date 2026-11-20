/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.common.conf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Validates and tokenizes JVM option strings and shell arguments to prevent command injection,
 * unauthorized agent loading, arbitrary file writes, and container heap overrides.
 */
public final class JvmOptionsValidator {

  private static final Pattern ENV_KEY_PATTERN = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");
  private static final Pattern SAFE_NON_PROXY_HOSTS_PATTERN =
      Pattern.compile("^[a-zA-Z0-9._*:\\-\\[\\]]+(\\|[a-zA-Z0-9._*:\\-\\[\\]]+)*$");
  private static final Pattern WHITESPACE_FLAG_SMUGGLE_PATTERN =
      Pattern.compile(".*\\s+[-@].*", Pattern.DOTALL);

  private static final Set<String> NON_PROXY_HOST_PROPERTIES = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(
          "http.nonproxyhosts",
          "https.nonproxyhosts",
          "ftp.nonproxyhosts",
          "socksnonproxyhosts"
      )));

  private static final List<String> FORBIDDEN_LAUNCHER_OPTION_PREFIXES =
      Collections.unmodifiableList(Arrays.asList(
          "-xx:onoutofmemoryerror",
          "-xx:onerror",
          "-xx:abortvmonexception",
          "-xx:abortvmonexceptionmessage",
          "-xx:vmoptionsfile",
          "-xx:flags",
          "-javaagent",
          "-agentlib",
          "-agentpath",
          "-xrun",
          "-xbootclasspath",
          "--patch-module",
          "-xpatch",
          "-xx:startattachlistener",
          "-xx:disableattachmechanism",
          "-xx:enabledynamicagentloading",
          "-xx:googleagentflags",
          "-xx:jvmcilibpath",
          "-xx:jvmcilibdumpjniconfig",
          "-xx:usejvmcinativelibrary",
          "-xx:enablejvmci",
          "-xx:tool"
      ));

  private static final List<String> FORBIDDEN_USER_OPTION_PREFIXES =
      Collections.unmodifiableList(Arrays.asList(
          "-xx:onoutofmemoryerror",
          "-xx:onerror",
          "-xx:abortvmonexception",
          "-xx:abortvmonexceptionmessage",
          "-xx:abortvmonvmoperationtimeout",
          "-xx:abortvmonvmoperationtimeoutdelay",
          "-xx:abortvmonsafepointtimeout",
          "-xx:abortvmonsafepointtimeoutdelay",
          "-xx:abortvmoncompilationfailure",
          "-xx:abortvmonaotcodefailure",
          "-xx:selfdestructtimer",
          "-xx:malloclimit",
          "-xx:mallocmaxtestwords",
          "-xx:vmoptionsfile",
          "-xx:flags",
          "-xx:unlockdiagnosticvmoptions",
          "-xx:unlockexperimentalvmoptions",
          "-xx:whiteboxapi",
          "-javaagent",
          "-agentlib",
          "-agentpath",
          "-xrun",
          "-xbootclasspath",
          "--patch-module",
          "-xpatch",
          "-xx:startattachlistener",
          "-xx:disableattachmechanism",
          "-xx:enabledynamicagentloading",
          "-xx:googleagentflags",
          "-xx:jvmcilibpath",
          "-xx:jvmcilibdumpjniconfig",
          "-xx:usejvmcinativelibrary",
          "-xx:enablejvmci",
          "-xx:tool",
          "-xx:errorfile",
          "-xx:heapdumppath",
          "-xx:heapdumponoutofmemoryerror",
          "-xx:heapdumpbeforefullgc",
          "-xx:heapdumpafterfullgc",
          "-xx:logfile",
          "-xlog",
          "-xx:logvmoutput",
          "-xx:logcompilation",
          "-xx:dumpperfmapatexit",
          "-xx:perfdatasavefile",
          "-xx:perfdatasavetofile",
          "-xx:replaydatafile",
          "-xx:dumpreplaydataonerror",
          "-xx:inlinedatafile",
          "-xx:compilerdirectivesfile",
          "-xx:allocateheapat",
          "-xx:pauseatstartup",
          "-xx:pauseatstartupfile",
          "-xx:pauseatexit",
          "-xx:startflightrecording",
          "-xx:flightrecorderoptions",
          "-xx:dumploadedclasslist",
          "-xx:sharedarchivefile",
          "-xx:sharedarchiveconfigfile",
          "-xx:sharedclasslistfile",
          "-xx:extrasharedclasslistfile",
          "-xx:archiveclassesatexit",
          "-xx:dynamicdumpsharedspaces",
          "-xx:dumpsharedspaces",
          "-xx:autocreatesharedarchive",
          "-xx:recorddynamicdumpinfo",
          "-xshare",
          "-xx:aotcache",
          "-xx:aotcacheoutput",
          "-xx:aotconfiguration",
          "-xx:aotmode",
          "-xx:compilecommand",
          "-xx:compilecommandfile",
          "-xmx",
          "-xms",
          "-xss",
          "-xmn",
          "-xminf",
          "-xmaxf",
          "-xmaxjitcodesize",
          "-xx:maxheapsize",
          "-xx:initialheapsize",
          "-xx:minheapsize",
          "-xx:softmaxheapsize",
          "-xx:ergoheapsizelimit",
          "-xx:newsize",
          "-xx:maxnewsize",
          "-xx:oldsize",
          "-xx:metaspacesize",
          "-xx:maxmetaspacesize",
          "-xx:compressedclassspacesize",
          "-xx:permsize",
          "-xx:maxpermsize",
          "-xx:threadstacksize",
          "-xx:vmthreadstacksize",
          "-xx:compilerthreadstacksize",
          "-xx:maxram",
          "-xx:maxrampercentage",
          "-xx:initialrampercentage",
          "-xx:minrampercentage",
          "-xx:maxramfraction",
          "-xx:initialramfraction",
          "-xx:minramfraction",
          "-xx:defaultmaxramfraction",
          "-xx:exitonoutofmemoryerror",
          "-xx:crashonoutofmemoryerror",
          "-xx:showmessageboxonerror",
          "-cp",
          "-classpath",
          "--class-path",
          "-jar",
          "-m",
          "--module",
          "--module-path",
          "-p",
          "--upgrade-module-path"
      ));

  private static final Set<String> FORBIDDEN_LAUNCHER_SYSTEM_PROPERTIES =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
          "jdk.instrument",
          "com.sun.management.jmxremote",
          "java.system.class.loader",
          "jdk.serialfilter",
          "jdk.serialfilterfactory"
      )));

  private static final Set<String> FORBIDDEN_USER_SYSTEM_PROPERTIES = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(
          "jdk.instrument",
          "com.sun.management.jmxremote",
          "java.library.path",
          "java.class.path",
          "java.ext.dirs",
          "java.endorsed.dirs",
          "java.system.class.loader",
          "jdk.serialfilter",
          "jdk.serialfilterfactory",
          "sun.boot.library.path",
          "sun.boot.class.path",
          "jna.library.path",
          "jna.boot.library.path",
          "org.xerial.snappy.lib.path",
          "org.xerial.snappy.lib.name",
          "org.xerial.snappy.tempdir",
          "io.netty.native.workdir",
          "javax.accessibility.assistive_technologies",
          "java.awt.toolkit",
          "java.nio.channels.spi.selectorprovider",
          "java.nio.file.spi.defaultfilesystemprovider",
          "logback.configurationfile",
          "log4j.configuration",
          "log4j.configurationfile",
          "log4j2.configurationfile",
          "java.util.logging.config.file",
          "java.util.logging.config.class",
          "cdap_log_dir",
          "user.dir",
          "java.io.tmpdir",
          "hive.classpath"
      )));

  private static final List<String> FORBIDDEN_LAUNCHER_SYSTEM_PROPERTY_PREFIXES =
      Collections.unmodifiableList(Arrays.asList(
          "jdk.instrument.",
          "jdk.attach.",
          "com.sun.management.jmxremote.",
          "com.sun.jndi.",
          "jdk.jndi."
      ));

  private static final List<String> FORBIDDEN_USER_SYSTEM_PROPERTY_PREFIXES =
      Collections.unmodifiableList(Arrays.asList(
          "jdk.instrument.",
          "jdk.attach.",
          "java.security.",
          "com.sun.management.jmxremote.",
          "java.rmi.",
          "sun.rmi.",
          "com.sun.jndi.",
          "jdk.jndi.",
          "javax.naming.",
          "logback.",
          "log4j.",
          "log4j2."
      ));

  private JvmOptionsValidator() {
  }

  /**
   * Validates all JVM option entries in a map of user arguments, preferences, or configurations.
   *
   * @param args map of configuration or runtime argument key-value pairs
   * @throws IllegalArgumentException if any JVM option entry contains disallowed flags or syntax
   */
  public static void validateJvmOptsMap(@Nullable Map<String, String> args) {
    if (args == null || args.isEmpty()) {
      return;
    }
    for (Map.Entry<String, String> entry : args.entrySet()) {
      if (isJvmOptionConfigKey(entry.getKey())) {
        validateUserJvmOptions(entry.getValue());
      }
    }
  }

  /**
   * Returns true if the given argument or configuration key configures JVM options.
   */
  public static boolean isJvmOptionConfigKey(@Nullable String key) {
    if (key == null || key.trim().isEmpty()) {
      return false;
    }
    String lower = key.trim().toLowerCase(Locale.ROOT);
    return lower.equals("system.program.jvm.opts")
        || lower.endsWith(".system.program.jvm.opts")
        || lower.equals(Constants.AppFabric.PROGRAM_JVM_OPTS)
        || lower.endsWith("." + Constants.AppFabric.PROGRAM_JVM_OPTS)
        || lower.startsWith(Constants.AppFabric.PROGRAM_JVM_OPTS_PREFIX)
        || lower.contains("." + Constants.AppFabric.PROGRAM_JVM_OPTS_PREFIX)
        || lower.equals("jvm.opts")
        || lower.endsWith(".jvm.opts")
        || lower.endsWith("extrajavaoptions")
        || lower.endsWith("defaultjavaoptions")
        || lower.endsWith(".java.opts")
        || lower.endsWith("command-opts")
        || lower.endsWith("jvm.gc.opts")
        || lower.equals("java_tool_options")
        || lower.endsWith(".java_tool_options")
        || lower.equals("_java_options")
        || lower.endsWith("._java_options")
        || lower.equals("jdk_java_options")
        || lower.endsWith(".jdk_java_options")
        || lower.equals("java_opts")
        || lower.endsWith(".java_opts");
  }

  /**
   * Validates user-supplied JVM options (e.g., from {@code system.program.jvm.opts}, preferences,
   * or user-configured Spark/MapReduce extra Java options).
   *
   * @param jvmOpts the user-supplied JVM options string
   * @throws IllegalArgumentException if the options contain disallowed flags, shell metacharacters,
   *     or malformed quotes
   */
  public static void validateUserJvmOptions(@Nullable String jvmOpts) {
    if (jvmOpts == null || jvmOpts.trim().isEmpty()) {
      return;
    }
    checkNoControlOrShellExpansion(jvmOpts);
    List<String> tokens = splitJvmOptions(jvmOpts);
    for (String token : tokens) {
      validateCommonToken(token, true);
      validateUserToken(token);
    }
  }

  /**
   * Validates launcher JVM options (after {@code <LOG_DIR>} substitution) before writing to a
   * remote launch script or container environment.
   *
   * @param jvmOpts the combined launcher JVM options string
   * @throws IllegalArgumentException if the options contain command-execution hooks, agent flags,
   *     or shell injection characters
   */
  public static void validateLauncherJvmOptions(@Nullable String jvmOpts) {
    if (jvmOpts == null || jvmOpts.trim().isEmpty()) {
      return;
    }
    checkNoControlOrShellExpansion(jvmOpts);
    List<String> tokens = splitJvmOptions(jvmOpts);
    for (String token : tokens) {
      validateCommonToken(token, false);
    }
  }

  /**
   * Splits a JVM options string into individual option tokens, honoring single and double quotes
   * without invoking a shell or expanding globs.
   *
   * @param jvmOpts the JVM options string to split
   * @return list of individual unquoted JVM option tokens
   * @throws IllegalArgumentException if quotes are unbalanced or control characters are present
   */
  public static List<String> splitJvmOptions(@Nullable String jvmOpts) {
    if (jvmOpts == null || jvmOpts.trim().isEmpty()) {
      return Collections.emptyList();
    }
    checkNoControlChars(jvmOpts);

    List<String> tokens = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    boolean inToken = false;
    char quoteChar = 0;

    for (int i = 0; i < jvmOpts.length(); i++) {
      char c = jvmOpts.charAt(i);
      if (quoteChar != 0) {
        if (c == '\\' && quoteChar == '"' && i + 1 < jvmOpts.length()) {
          char next = jvmOpts.charAt(++i);
          current.append(next);
        } else if (c == quoteChar) {
          quoteChar = 0;
        } else {
          current.append(c);
        }
      } else {
        if (c == '\\') {
          throw new IllegalArgumentException(
              "Invalid JVM options: unquoted backslash escape is not allowed in '" + jvmOpts + "'");
        } else if (c == '"' || c == '\'') {
          quoteChar = c;
          inToken = true;
        } else if (Character.isWhitespace(c)) {
          if (inToken) {
            tokens.add(current.toString());
            current.setLength(0);
            inToken = false;
          }
        } else {
          current.append(c);
          inToken = true;
        }
      }
    }

    if (quoteChar != 0) {
      throw new IllegalArgumentException(
          "Invalid JVM options: unbalanced quote in '" + jvmOpts + "'");
    }
    if (inToken) {
      tokens.add(current.toString());
    }
    return tokens;
  }

  /**
   * Validates that an environment variable key is a safe POSIX shell identifier.
   */
  public static void validateEnvKey(String key) {
    if (key == null || !ENV_KEY_PATTERN.matcher(key).matches()) {
      throw new IllegalArgumentException("Invalid environment variable name: '" + key + "'");
    }
  }

  /**
   * Quotes a string as a single-quoted POSIX shell literal ({@code '...'}), escaping embedded
   * single quotes as {@code '\''} and rejecting control characters.
   */
  public static String shellQuote(String value) {
    if (value == null) {
      throw new IllegalArgumentException("Shell argument value cannot be null");
    }
    checkNoControlChars(value);
    return "'" + value.replace("'", "'\\''") + "'";
  }

  private static void checkNoControlChars(String value) {
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if ((c < 0x20 && c != '\t') || c == 0x7f) {
        throw new IllegalArgumentException(
            "Invalid JVM options or shell argument: control characters are not allowed");
      }
    }
  }

  private static void checkNoControlOrShellExpansion(String jvmOpts) {
    checkNoControlChars(jvmOpts);
    if (jvmOpts.indexOf('`') >= 0 || jvmOpts.indexOf('$') >= 0) {
      throw new IllegalArgumentException(
          "Invalid JVM options: shell command or variable expansion is not allowed");
    }
  }

  private static void validateCommonToken(String token, boolean strictShellMeta) {
    if (token.isEmpty() || !token.startsWith("-")) {
      throw new IllegalArgumentException(
          "Invalid JVM option '" + token + "': every option must start with '-'");
    }
    if (token.indexOf(';') >= 0 || token.indexOf('&') >= 0
        || token.indexOf('`') >= 0 || token.indexOf('$') >= 0
        || token.indexOf('(') >= 0 || token.indexOf(')') >= 0) {
      throw new IllegalArgumentException(
          "Invalid JVM option '" + token + "': shell metacharacters are not allowed");
    }
    if (strictShellMeta && (token.indexOf('>') >= 0 || token.indexOf('<') >= 0)) {
      throw new IllegalArgumentException(
          "Invalid JVM option '" + token + "': redirection characters are not allowed");
    }
    if (token.indexOf('|') >= 0 && !isAllowedNonProxyHostsToken(token)) {
      throw new IllegalArgumentException(
          "Invalid JVM option '" + token + "': pipe character is not allowed");
    }

    String lower = token.toLowerCase(Locale.ROOT);
    for (String prefix : FORBIDDEN_LAUNCHER_OPTION_PREFIXES) {
      if (matchesFlagPrefix(lower, prefix)) {
        throw new IllegalArgumentException(
            "Invalid JVM option '" + token + "': option is not permitted");
      }
    }
    if (lower.startsWith("-d")) {
      validateSystemPropertyToken(token, strictShellMeta);
    }
  }

  private static void validateUserToken(String token) {
    String lower = token.toLowerCase(Locale.ROOT);
    for (String prefix : FORBIDDEN_USER_OPTION_PREFIXES) {
      if (matchesFlagPrefix(lower, prefix)) {
        throw new IllegalArgumentException(
            "Invalid JVM option '" + token + "': option is not permitted in user arguments");
      }
    }
  }

  private static void validateSystemPropertyToken(String token, boolean isUserOpts) {
    String propPart = token.substring(2);
    int eqIdx = propPart.indexOf('=');
    String propKey = eqIdx >= 0 ? propPart.substring(0, eqIdx) : propPart;
    if (propKey.isEmpty() || containsWhitespaceOrQuote(propKey)) {
      throw new IllegalArgumentException(
          "Invalid JVM system property option '" + token + "': invalid property name");
    }
    String propVal = eqIdx >= 0 ? propPart.substring(eqIdx + 1) : "";
    if (isUserOpts && WHITESPACE_FLAG_SMUGGLE_PATTERN.matcher(propVal).matches()) {
      throw new IllegalArgumentException(
          "Invalid JVM system property option '" + token
              + "': embedded option flags in property values are not allowed");
    }
    String lowerKey = propKey.toLowerCase(Locale.ROOT);
    Set<String> forbiddenProps = isUserOpts
        ? FORBIDDEN_USER_SYSTEM_PROPERTIES : FORBIDDEN_LAUNCHER_SYSTEM_PROPERTIES;
    if (forbiddenProps.contains(lowerKey)) {
      throw new IllegalArgumentException(
          "Invalid JVM system property '" + propKey + "': property is restricted");
    }
    List<String> forbiddenPrefixes = isUserOpts
        ? FORBIDDEN_USER_SYSTEM_PROPERTY_PREFIXES : FORBIDDEN_LAUNCHER_SYSTEM_PROPERTY_PREFIXES;
    for (String prefix : forbiddenPrefixes) {
      if (lowerKey.startsWith(prefix)) {
        throw new IllegalArgumentException(
            "Invalid JVM system property '" + propKey + "': property prefix is restricted");
      }
    }
    if (isJvmOptionConfigKey(propKey)) {
      validateUserJvmOptions(propVal);
    }
  }

  private static boolean containsWhitespaceOrQuote(String s) {
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (Character.isWhitespace(c) || c == '"' || c == '\'' || c == '\\') {
        return true;
      }
    }
    return false;
  }

  private static boolean isAllowedNonProxyHostsToken(String token) {
    if (!token.startsWith("-D") && !token.startsWith("-d")) {
      return false;
    }
    String propPart = token.substring(2);
    int eqIdx = propPart.indexOf('=');
    if (eqIdx <= 0 || eqIdx == propPart.length() - 1) {
      return false;
    }
    String propKey = propPart.substring(0, eqIdx).toLowerCase(Locale.ROOT);
    String propVal = propPart.substring(eqIdx + 1);
    return NON_PROXY_HOST_PROPERTIES.contains(propKey)
        && SAFE_NON_PROXY_HOSTS_PATTERN.matcher(propVal).matches();
  }

  private static boolean matchesFlagPrefix(String lowerToken, String lowerPrefix) {
    String normalizedToken = lowerToken;
    if (lowerPrefix.startsWith("-xx:")
        && (lowerToken.startsWith("-xx:+") || lowerToken.startsWith("-xx:-"))) {
      normalizedToken = "-xx:" + lowerToken.substring(5);
    }
    if (normalizedToken.equals(lowerPrefix)) {
      return true;
    }
    if (!normalizedToken.startsWith(lowerPrefix)) {
      return false;
    }
    // Flags like -Xmx8m, -Xms8m, -Xss256k, -Xmn8m, -Xrunhprof attach the value directly
    if (lowerPrefix.equals("-xmx") || lowerPrefix.equals("-xms")
        || lowerPrefix.equals("-xss") || lowerPrefix.equals("-xmn")
        || lowerPrefix.equals("-xminf") || lowerPrefix.equals("-xmaxf")
        || lowerPrefix.equals("-xmaxjitcodesize") || lowerPrefix.equals("-xshare")
        || lowerPrefix.equals("-xrun") || lowerPrefix.equals("-xlog")) {
      return true;
    }
    char next = normalizedToken.charAt(lowerPrefix.length());
    return next == '=' || next == ':' || next == '/';
  }
}
