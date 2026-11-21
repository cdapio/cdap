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

package io.cdap.cdap.internal.app.runtime.plugin;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeScopes;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TriggerMappedArguments;
import io.cdap.cdap.proto.id.ProgramId;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extracts the {@link RuntimeScopes} (connections, OAuth credentials, secrets, and dynamic-secret
 * flag) reachable by a program run from its deployed {@link ApplicationSpecification}, resolved
 * user arguments, trigger-mapped arguments, and referenced connections.
 */
public final class RuntimeScopesExtractor {

  private static final Logger LOG = LoggerFactory.getLogger(RuntimeScopesExtractor.class);

  // Macro function names owned by cdap-etl-core, which app-fabric cannot depend on. Keep in sync
  // with SecureStoreMacroEvaluator, ConnectionMacroEvaluator, OAuthMacroEvaluator, and
  // OAuthAccessTokenMacroEvaluator.
  private static final String MACRO_SECURE = "secure";
  private static final String MACRO_CONN = "conn";
  private static final String MACRO_OAUTH = "oauth";
  private static final String MACRO_OAUTH_ACCESS_TOKEN = "oauthAccessToken";

  // Dynamic plugin types owned by cdap-etl-api / cdap-etl-core, which app-fabric cannot depend on.
  // Keep in sync with Action.PLUGIN_TYPE, PostAction.PLUGIN_TYPE, Condition.PLUGIN_TYPE, and
  // io.cdap.cdap.etl.common.Constants.SPARK_PROGRAM_PLUGIN_TYPE.
  private static final Set<String> DYNAMIC_PLUGIN_TYPES = ImmutableSet.of(
      "action",
      "postaction",
      "condition",
      "sparkprogram");

  // Runtime argument scope prefixes that can override a lookup key on the cluster.
  // Keep in sync with io.cdap.cdap.api.common.Scope (spark, mapreduce),
  // WorkflowDriver.ACTION_SCOPE (action), and task-scoped MapReduce arguments (task).
  private static final List<String> SCOPED_OVERRIDE_PREFIXES = ImmutableList.of(
      "spark.",
      "mapreduce.",
      "action.",
      "task.");

  // Private-use placeholder characters standing in for unknown lookup values and function results.
  static final char LOOKUP_MARKER = '\uE000'; // private-use placeholder for unresolved lookup
  static final char FUNCTION_MARKER = '\uE001'; // private-use placeholder for function result
  private static final String LOOKUP_MARKER_STR = String.valueOf(LOOKUP_MARKER);
  private static final String FUNCTION_MARKER_STR = String.valueOf(FUNCTION_MARKER);

  private static final MacroParserOptions ESCAPING_ON =
      MacroParserOptions.builder().setEscaping(true).build();
  private static final MacroParserOptions ESCAPING_OFF =
      MacroParserOptions.builder().setEscaping(false).build();

  private RuntimeScopesExtractor() {
  }

  /**
   * Reads the plugin properties of a referenced connection at launch time.
   */
  @FunctionalInterface
  public interface ConnectionReader {

    /**
     * Reads the properties of the named connection.
     *
     * @param connectionName the connection name as written in the {@code ${conn(...)}} macro
     * @return the connection's properties, or {@link Optional#empty()} if the connection was not
     *     found (404) or rejected as invalid (400)
     * @throws IOException if reading the connection failed transiently or unexpectedly
     */
    Optional<Map<String, String>> read(String connectionName) throws IOException;
  }

  /**
   * Computes {@link RuntimeScopes} for a program run. Never throws; unreadable trigger mappings
   * or unexpected errors fall back to {@link RuntimeScopes#wildcardAll()} with a warning log.
   *
   * @param appSpec deployed application specification
   * @param programId program being launched
   * @param resolvedUserArgs user arguments after cluster/application/program scope resolution
   * @param triggerMappedArgs result of {@link TriggerMappedArguments#extract}
   * @param connectionReader reader for resolving referenced connections' properties
   * @return extracted immutable {@link RuntimeScopes}
   */
  public static RuntimeScopes extract(ApplicationSpecification appSpec,
      ProgramId programId,
      @Nullable Map<String, String> resolvedUserArgs,
      @Nullable TriggerMappedArguments.Result triggerMappedArgs,
      ConnectionReader connectionReader) {
    try {
      Objects.requireNonNull(appSpec, "appSpec");
      Objects.requireNonNull(programId, "programId");
      Objects.requireNonNull(connectionReader, "connectionReader");

      if (triggerMappedArgs != null && triggerMappedArgs.isUnreadable()) {
        LOG.warn("Trigger mapping for program {} is unreadable; widening runtime scopes",
            programId);
        return RuntimeScopes.wildcardAll();
      }

      Map<String, String> userArgs = resolvedUserArgs == null
          ? Collections.emptyMap() : resolvedUserArgs;
      Map<String, String> triggerArgs = triggerMappedArgs == null
          ? Collections.emptyMap() : triggerMappedArgs.getArguments();

      ExtractorSession session = new ExtractorSession(
          programId, userArgs, triggerArgs, connectionReader);
      return session.run(appSpec);
    } catch (Exception | StackOverflowError e) {
      LOG.warn("Unexpected error while extracting runtime scopes for program {}; widening scopes",
          programId, e);
      return RuntimeScopes.wildcardAll();
    }
  }

  private static final class ExtractorSession {

    private final ProgramId programId;
    private final Map<String, String> userArgs;
    private final Map<String, String> triggerArgs;
    private final ConnectionReader connectionReader;
    private final List<String> remainingScopedKeys;

    private final Set<String> connections = new LinkedHashSet<>();
    private final Set<String> oauth = new LinkedHashSet<>();
    private final Set<String> secrets = new LinkedHashSet<>();
    private boolean hasExternalDynamicSecret;

    private final Set<String> queuedConnectionIds = new HashSet<>();
    private final Deque<QueuedConnection> pendingConnections = new ArrayDeque<>();

    private final List<MacroParser> parsers;

    private ExtractorSession(ProgramId programId, Map<String, String> userArgs,
        Map<String, String> triggerArgs, ConnectionReader connectionReader) {
      this.programId = programId;
      this.userArgs = userArgs;
      this.triggerArgs = triggerArgs;
      this.connectionReader = connectionReader;
      this.remainingScopedKeys = collectRemainingScopedKeys(programId, userArgs, triggerArgs);

      ImmutableList.Builder<MacroParser> parserBuilder = ImmutableList.builder();
      RecordingEvaluator resolvedEvaluator = new RecordingEvaluator(false);
      parserBuilder.add(new MacroParser(resolvedEvaluator, ESCAPING_ON));
      parserBuilder.add(new MacroParser(resolvedEvaluator, ESCAPING_OFF));
      if (SystemArguments.skipNormalMacroEvaluation(userArgs)) {
        RecordingEvaluator unknownEvaluator = new RecordingEvaluator(true);
        parserBuilder.add(new MacroParser(unknownEvaluator, ESCAPING_ON));
        parserBuilder.add(new MacroParser(unknownEvaluator, ESCAPING_OFF));
      }
      this.parsers = parserBuilder.build();
    }

    private RuntimeScopes run(ApplicationSpecification appSpec) {
      if (appSpec.getArtifactId() != null
          && appSpec.getArtifactId().getScope() == ArtifactScope.USER) {
        hasExternalDynamicSecret = true;
      }

      Map<String, Plugin> plugins = appSpec.getPlugins();
      if (plugins != null) {
        for (Plugin plugin : plugins.values()) {
          if (plugin == null) {
            continue;
          }
          if (plugin.getPluginClass() != null
              && DYNAMIC_PLUGIN_TYPES.contains(plugin.getPluginClass().getType())) {
            hasExternalDynamicSecret = true;
          }
          if (plugin.getProperties() != null && plugin.getProperties().getProperties() != null) {
            for (String value : plugin.getProperties().getProperties().values()) {
              scanText(value);
            }
          }
        }
      }

      scanConfiguration(appSpec.getConfiguration());

      for (String value : userArgs.values()) {
        scanText(value);
      }
      for (String value : triggerArgs.values()) {
        scanText(value);
      }

      drainConnectionQueue();

      return new RuntimeScopes(connections, oauth, secrets, hasExternalDynamicSecret);
    }

    private void scanConfiguration(@Nullable String configuration) {
      if (Strings.isNullOrEmpty(configuration)) {
        return;
      }
      // Stream tokens iteratively with JsonReader to avoid recursive DOM parsing on deep JSON
      // and to capture both JSON object keys and string values.
      if (!scanJsonTokens(configuration, false)) {
        // Strict JSON parsing failed: try lenient JSON (matching Gson.fromJson) and also scan
        // the raw text so non-JSON or malformed configurations are still covered.
        scanJsonTokens(configuration, true);
        scanText(configuration);
      }
    }

    private boolean scanJsonTokens(String configuration, boolean lenient) {
      try (JsonReader reader = new JsonReader(new StringReader(configuration))) {
        reader.setLenient(lenient);
        JsonToken token;
        while ((token = reader.peek()) != JsonToken.END_DOCUMENT) {
          switch (token) {
            case BEGIN_OBJECT:
              reader.beginObject();
              break;
            case END_OBJECT:
              reader.endObject();
              break;
            case BEGIN_ARRAY:
              reader.beginArray();
              break;
            case END_ARRAY:
              reader.endArray();
              break;
            case NAME:
              scanText(reader.nextName());
              break;
            case STRING:
              scanText(reader.nextString());
              break;
            case NUMBER:
              reader.nextString();
              break;
            case BOOLEAN:
              reader.nextBoolean();
              break;
            case NULL:
              reader.nextNull();
              break;
            default:
              reader.skipValue();
              break;
          }
        }
        return true;
      } catch (IOException | IllegalStateException | NumberFormatException e) {
        return false;
      }
    }

    private void scanText(@Nullable String rawText) {
      if (rawText == null) {
        return;
      }
      String sanitized = stripMarkers(rawText);
      if (!sanitized.contains("${")) {
        return;
      }
      for (MacroParser parser : parsers) {
        try {
          parser.parse(sanitized);
        } catch (InvalidMacroException e) {
          // Parse errors stop evaluation of this text; recordings made before the error stay.
        }
      }
    }

    private void drainConnectionQueue() {
      while (!pendingConnections.isEmpty()) {
        QueuedConnection queued = pendingConnections.removeFirst();
        Optional<Map<String, String>> props;
        try {
          props = Objects.requireNonNull(connectionReader.read(queued.macroName),
              "ConnectionReader returned null");
        } catch (Exception | StackOverflowError e) {
          LOG.warn("Failed to read connection '{}' for program {}; widening scopes",
              queued.connectionId, programId, e);
          widenUnknownConnection();
          continue;
        }
        if (!props.isPresent()) {
          continue;
        }
        for (String value : props.get().values()) {
          scanText(value);
        }
      }
    }

    private String resolveLookup(String key) {
      if (isShadowedByScopedOverride(key)) {
        return LOOKUP_MARKER_STR;
      }
      String userVal = userArgs.get(key);
      String triggerVal = triggerArgs.get(key);
      if (userVal != null && triggerVal != null) {
        return userVal.equals(triggerVal) ? stripMarkers(userVal) : LOOKUP_MARKER_STR;
      }
      if (userVal != null) {
        return stripMarkers(userVal);
      }
      if (triggerVal != null) {
        return stripMarkers(triggerVal);
      }
      return LOOKUP_MARKER_STR;
    }

    private boolean isShadowedByScopedOverride(String key) {
      if (remainingScopedKeys.isEmpty() || key.isEmpty()) {
        return false;
      }
      String dotKey = "." + key;
      for (String argKey : remainingScopedKeys) {
        if (!argKey.endsWith(dotKey)) {
          continue;
        }
        for (String prefix : SCOPED_OVERRIDE_PREFIXES) {
          // Require a non-empty scope target between "<prefix>" and ".<key>" (e.g. "spark.job.k").
          if (argKey.startsWith(prefix)
              && argKey.length() > prefix.length() + dotKey.length()) {
            return true;
          }
        }
      }
      return false;
    }

    private void recordFunction(String macroFunction, String[] args) {
      if (containsMarker(macroFunction)) {
        // An unresolved function name could evaluate to conn(...), oauth(...), or secure(...).
        widenUnknownConnection();
        return;
      }
      switch (macroFunction) {
        case MACRO_SECURE:
          recordSecure(args);
          break;
        case MACRO_CONN:
          recordConnection(args);
          break;
        case MACRO_OAUTH:
        case MACRO_OAUTH_ACCESS_TOKEN:
          recordOauth(args);
          break;
        default:
          // Other macro functions (e.g. logicalStartTime) are not evaluated.
          break;
      }
    }

    private void recordSecure(String[] args) {
      if (args.length != 1) {
        if (anyArgContainsMarker(args)) {
          hasExternalDynamicSecret = true;
        }
        return;
      }
      String name = args[0];
      if (name == null || name.isEmpty() || name.indexOf('*') >= 0) {
        return;
      }
      if (name.indexOf(FUNCTION_MARKER) >= 0) {
        hasExternalDynamicSecret = true;
        return;
      }
      if (name.indexOf(LOOKUP_MARKER) >= 0) {
        if (isOnlyLookupMarkers(name)) {
          hasExternalDynamicSecret = true;
        } else {
          secrets.add(name.replace(LOOKUP_MARKER, '*'));
        }
        return;
      }
      secrets.add(name);
    }

    private void recordConnection(String[] args) {
      if (args.length != 1) {
        if (anyArgContainsMarker(args)) {
          widenUnknownConnection();
        }
        return;
      }
      String connName = args[0];
      if (connName == null) {
        return;
      }
      if (containsMarker(connName)) {
        widenUnknownConnection();
        return;
      }
      Optional<String> connectionId = RuntimeScopes.connectionIdFromMacroName(connName);
      if (!connectionId.isPresent()) {
        return;
      }
      String id = connectionId.get();
      connections.add(id);
      if (queuedConnectionIds.add(id)) {
        pendingConnections.addLast(new QueuedConnection(id, connName));
      }
    }

    private void recordOauth(String[] args) {
      if (args.length != 2) {
        if (anyArgContainsMarker(args)) {
          oauth.add(RuntimeScopes.OAUTH_WILDCARD);
        }
        return;
      }
      String provider = args[0];
      String credential = args[1];
      if (Strings.isNullOrEmpty(provider) || Strings.isNullOrEmpty(credential)
          || provider.indexOf('*') >= 0 || provider.indexOf('/') >= 0
          || credential.indexOf('*') >= 0 || credential.indexOf('/') >= 0) {
        return;
      }
      String recordedProvider = containsMarker(provider) ? RuntimeScopes.WILDCARD : provider;
      String recordedCredential = containsMarker(credential) ? RuntimeScopes.WILDCARD : credential;
      oauth.add(recordedProvider + "/" + recordedCredential);
    }

    private void widenUnknownConnection() {
      connections.add(RuntimeScopes.WILDCARD);
      oauth.add(RuntimeScopes.OAUTH_WILDCARD);
      hasExternalDynamicSecret = true;
    }

    private final class RecordingEvaluator implements MacroEvaluator {

      private final boolean treatAllLookupsUnknown;

      private RecordingEvaluator(boolean treatAllLookupsUnknown) {
        this.treatAllLookupsUnknown = treatAllLookupsUnknown;
      }

      @Override
      public String lookup(String property) {
        if (treatAllLookupsUnknown || property == null) {
          return LOOKUP_MARKER_STR;
        }
        return resolveLookup(property);
      }

      @Override
      public String evaluate(String macroFunction, String... arguments) {
        if (macroFunction != null) {
          recordFunction(macroFunction, arguments == null ? new String[0] : arguments);
        }
        return FUNCTION_MARKER_STR;
      }
    }
  }

  private static List<String> collectRemainingScopedKeys(ProgramId programId,
      Map<String, String> userArgs, Map<String, String> triggerArgs) {
    String ownScope = programId.getType().getScope();
    String ownExactPrefix = ownScope + "." + programId.getProgram() + ".";
    String ownWildcardPrefix = ownScope + ".*.";

    Set<String> allKeys = new HashSet<>(userArgs.keySet());
    allKeys.addAll(triggerArgs.keySet());

    List<String> scopedKeys = new ArrayList<>();
    for (String key : allKeys) {
      if (key == null || key.startsWith(ownExactPrefix) || key.startsWith(ownWildcardPrefix)) {
        continue;
      }
      for (String prefix : SCOPED_OVERRIDE_PREFIXES) {
        if (key.startsWith(prefix)) {
          scopedKeys.add(key);
          break;
        }
      }
    }
    return scopedKeys;
  }

  private static String stripMarkers(String s) {
    if (s.indexOf(LOOKUP_MARKER) < 0 && s.indexOf(FUNCTION_MARKER) < 0) {
      return s;
    }
    StringBuilder sb = new StringBuilder(s.length());
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c != LOOKUP_MARKER && c != FUNCTION_MARKER) {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  private static boolean containsMarker(@Nullable String s) {
    return s != null && (s.indexOf(LOOKUP_MARKER) >= 0 || s.indexOf(FUNCTION_MARKER) >= 0);
  }

  private static boolean anyArgContainsMarker(String[] args) {
    for (String arg : args) {
      if (containsMarker(arg)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isOnlyLookupMarkers(String s) {
    for (int i = 0; i < s.length(); i++) {
      if (s.charAt(i) != LOOKUP_MARKER) {
        return false;
      }
    }
    return !s.isEmpty();
  }

  private static final class QueuedConnection {

    private final String connectionId;
    private final String macroName;

    private QueuedConnection(String connectionId, String macroName) {
      this.connectionId = connectionId;
      this.macroName = macroName;
    }
  }
}
