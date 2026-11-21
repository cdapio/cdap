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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.lib.FileSetProperties;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.data2.dataset2.lib.external.ExternalDataset;
import io.cdap.cdap.proto.DatasetInstanceConfiguration;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Callback API allowlist for {@code RUNTIME}-token requests routed by the Runtime Service.
 *
 * <p>Loads the bundled allowlist from {@value #BUNDLED_ALLOWLIST_RESOURCE} and optionally appends
 * extra rules configured via {@link Constants.RuntimeMonitor#RUNTIME_TOKEN_ALLOWLIST_EXTRA_RULES}.
 */
@Singleton
final class RuntimeCallbackAllowlist {

  static final String BUNDLED_ALLOWLIST_RESOURCE = "runtime-callback-allowlist.json";
  static final String DEFAULT_CHECKPOINT_DATASET = "defaultCheckpointDataset";

  private static final Logger LOG = LoggerFactory.getLogger(RuntimeCallbackAllowlist.class);
  private static final Gson GSON = new Gson();
  private static final Set<String> ALLOWED_RULE_KEYS = ImmutableSet.of(
      "service", "methods", "pathTemplate", "namespaceRule", "applicationRule", "bodyRule",
      "scopeKind");
  private static final Set<String> SUPPORTED_METHODS = ImmutableSet.of(
      "GET", "POST", "PUT", "DELETE");
  private static final Set<String> BODY_METHODS = ImmutableSet.of("PUT", "POST");
  private static final List<String> FORBIDDEN_FILESET_PROPERTY_SUFFIXES = ImmutableList.of(
      FileSetProperties.BASE_PATH,
      FileSetProperties.DATA_EXTERNAL,
      FileSetProperties.DATA_USE_EXISTING,
      FileSetProperties.DATA_POSSESS_EXISTING);

  private final ListMultimap<String, Rule> rulesByService;
  private final boolean kerberosEnabled;

  private enum NamespaceRule {
    NONE,
    RUN,
    RUN_OR_SYSTEM
  }

  private enum ApplicationRule {
    NONE,
    RUN
  }

  private enum BodyRule {
    NONE,
    DATASET_PUT,
    DATASET_DELETE
  }

  private enum ScopeKind {
    NONE(null, null),
    SECRET(RuntimeValidationFailure.SECRET, RuntimeScopeAdmission.DYNAMIC_SECRET, "name"),
    CONNECTION(RuntimeValidationFailure.CONNECTION, RuntimeScopeAdmission.WILDCARD_CONNECTION,
        "connection"),
    OAUTH(RuntimeValidationFailure.OAUTH, RuntimeScopeAdmission.WILDCARD_OAUTH,
        "provider", "credential");

    @Nullable
    private final RuntimeValidationFailure failure;
    @Nullable
    private final RuntimeScopeAdmission admission;
    private final List<String> requiredCaptures;

    ScopeKind(@Nullable RuntimeValidationFailure failure,
        @Nullable RuntimeScopeAdmission admission, String... requiredCaptures) {
      this.failure = failure;
      this.admission = admission;
      this.requiredCaptures = ImmutableList.copyOf(requiredCaptures);
    }
  }

  @Inject
  RuntimeCallbackAllowlist(CConfiguration cConf) {
    this(cConf, LOG);
  }

  RuntimeCallbackAllowlist(CConfiguration cConf, Logger logger) {
    List<Rule> allRules = new ArrayList<>(loadBundledRules());
    allRules.addAll(loadExtraRules(cConf, logger));

    ImmutableListMultimap.Builder<String, Rule> indexBuilder = ImmutableListMultimap.builder();
    for (Rule rule : allRules) {
      indexBuilder.put(rule.service, rule);
    }
    this.rulesByService = indexBuilder.build();
    this.kerberosEnabled = SecurityUtil.isKerberosEnabled(cConf);
  }

  private static List<Rule> loadExtraRules(CConfiguration cConf, Logger logger) {
    String key = Constants.RuntimeMonitor.RUNTIME_TOKEN_ALLOWLIST_EXTRA_RULES;
    try {
      String extraRulesConfig = cConf.getTrimmed(key);
      if (Strings.isNullOrEmpty(extraRulesConfig)) {
        return Collections.emptyList();
      }
      List<Rule> extraRules = parseRules(new StringReader(extraRulesConfig), key);
      if (!extraRules.isEmpty()) {
        logger.warn("Loaded {} extra runtime callback allowlist rule(s) from {}",
            extraRules.size(), key);
      }
      return extraRules;
    } catch (IllegalArgumentException | IllegalStateException e) {
      logger.error("Ignoring invalid {}. Only the bundled runtime callback allowlist is in effect.",
          key, e);
      return Collections.emptyList();
    }
  }

  /**
   * Matches an incoming {@code RUNTIME}-token service routing request against the allowlist and,
   * when {@code runtimeScopes} is non-null, enforces per-run scope checks for scoped rules.
   *
   * <p>Validates and decodes {@code servicePath}, matches path segments against configured
   * templates, and verifies namespace, application, path-only body rules (such as
   * {@code DATASET_DELETE}), and scope rules against {@code tokenRunId} and {@code runtimeScopes}.
   * For rules with {@link BodyRule#DATASET_PUT}, the caller must additionally buffer and verify
   * the request body with {@link #checkBody(MatchResult, ProgramRunId, byte[])}.
   */
  MatchResult match(String servicePath, String service, String method,
      ProgramRunId tokenRunId, @Nullable RuntimeScopes runtimeScopes) {
    List<String> decodedSegments = parseAndValidateServicePath(servicePath);
    if (decodedSegments == null) {
      return MatchResult.denied(
          RuntimeValidationFailure.API_NOT_ALLOWED, extractLogPath(servicePath));
    }

    List<Rule> serviceRules = rulesByService.get(service);
    if (serviceRules.isEmpty()) {
      return MatchResult.denied(
          RuntimeValidationFailure.API_NOT_ALLOWED, extractLogPath(servicePath));
    }

    String normalizedMethod = method.toUpperCase(Locale.ROOT);
    String matchedTemplate = null;
    RuntimeValidationFailure scopeFailure = null;
    String scopeFailureTemplate = null;
    MatchResult fallbackCandidate = null;
    for (Rule rule : serviceRules) {
      if (!rule.methods.contains(normalizedMethod)) {
        continue;
      }
      Map<String, String> captures = rule.matchSegments(decodedSegments);
      if (captures == null) {
        continue;
      }
      if (matchedTemplate == null) {
        matchedTemplate = rule.pathTemplate;
      }
      if (!checkNamespace(rule.namespaceRule, captures.get("namespace"), tokenRunId)) {
        continue;
      }
      if (!checkApplication(rule.applicationRule, captures.get("app"), tokenRunId)) {
        continue;
      }
      if (rule.bodyRule == BodyRule.DATASET_DELETE
          && !isWorkflowLocalDataset(captures.get("name"), tokenRunId)) {
        continue;
      }

      RuntimeScopeAdmission admission = null;
      if (rule.scopeKind != ScopeKind.NONE && runtimeScopes != null) {
        RuntimeScopes.ScopeMatch scopeMatch = checkScope(rule.scopeKind, captures, runtimeScopes);
        if (scopeMatch == RuntimeScopes.ScopeMatch.DENIED) {
          if (scopeFailure == null) {
            scopeFailure = rule.scopeKind.failure;
            scopeFailureTemplate = rule.pathTemplate;
          }
          continue;
        }
        if (scopeMatch == RuntimeScopes.ScopeMatch.ALLOWED_BY_WILDCARD) {
          admission = rule.scopeKind.admission;
        }
      }

      if (rule.bodyRule == BodyRule.DATASET_PUT || admission != null) {
        // Keep scanning so a later unconditional rule (e.g. from extra.rules) can allow without
        // body checks or wildcard admission.
        if (fallbackCandidate == null) {
          fallbackCandidate = MatchResult.allowed(
              rule.pathTemplate, rule.bodyRule, captures, admission);
        }
        continue;
      }
      return MatchResult.allowed(rule.pathTemplate, rule.bodyRule, captures, null);
    }
    if (fallbackCandidate != null) {
      return fallbackCandidate;
    }
    if (scopeFailure != null) {
      return MatchResult.denied(scopeFailure, scopeFailureTemplate);
    }
    return MatchResult.denied(RuntimeValidationFailure.API_NOT_ALLOWED,
        matchedTemplate != null ? matchedTemplate : extractLogPath(servicePath));
  }

  /**
   * Verifies the buffered request body for a matched rule that requires body validation.
   */
  boolean checkBody(MatchResult match, ProgramRunId tokenRunId, byte[] bodyBytes) {
    if (match.bodyRule != BodyRule.DATASET_PUT) {
      return true;
    }
    if (bodyBytes.length == 0) {
      return false;
    }
    DatasetInstanceConfiguration config;
    try (Reader reader = new InputStreamReader(
        new ByteArrayInputStream(bodyBytes), StandardCharsets.UTF_8)) {
      config = GSON.fromJson(reader, DatasetInstanceConfiguration.class);
    } catch (JsonParseException | IOException e) {
      return false;
    }
    if (config == null) {
      return false;
    }
    if (!kerberosEnabled && config.getOwnerPrincipal() != null) {
      return false;
    }
    String typeName = config.getTypeName();
    if (Strings.isNullOrEmpty(typeName)) {
      return false;
    }
    if (ExternalDataset.TYPE.equals(typeName)) {
      return true;
    }
    String datasetName = match.getCapture("name");
    if (datasetName == null) {
      return false;
    }
    boolean allowedTarget = isWorkflowLocalDataset(datasetName, tokenRunId)
        || (DEFAULT_CHECKPOINT_DATASET.equals(datasetName) && isFileSetType(typeName));
    if (!allowedTarget) {
      return false;
    }
    return !hasForbiddenFileSetProperty(config.getProperties());
  }

  /**
   * Parses and validates {@code servicePath}.
   *
   * <p>Requires {@code servicePath} to be a relative URI starting with {@code "/"} with no scheme,
   * authority, fragment, empty segments, or {@code .}/{@code ..} segments (allowing a single
   * trailing slash), and decode without {@code /}, {@code \}, or ISO control characters.
   *
   * @return decoded path segments if valid, or {@code null} otherwise
   */
  @Nullable
  private static List<String> parseAndValidateServicePath(String servicePath) {
    if (servicePath.startsWith("//")) {
      return null;
    }
    URI uri;
    try {
      uri = URI.create(servicePath);
    } catch (IllegalArgumentException e) {
      return null;
    }
    if (uri.getScheme() != null || uri.getRawAuthority() != null || uri.getRawFragment() != null) {
      return null;
    }
    String rawPath = uri.getRawPath();
    if (rawPath == null || !rawPath.startsWith("/")) {
      return null;
    }
    if (rawPath.length() > 1 && rawPath.endsWith("/")) {
      rawPath = rawPath.substring(0, rawPath.length() - 1);
    }
    String[] rawSegments = rawPath.substring(1).split("/", -1);
    List<String> decodedSegments = new ArrayList<>(rawSegments.length);
    for (String rawSegment : rawSegments) {
      if (rawSegment.isEmpty() || ".".equals(rawSegment) || "..".equals(rawSegment)) {
        return null;
      }
      String decoded = decodeSegment(rawSegment);
      if (decoded == null || decoded.isEmpty() || ".".equals(decoded) || "..".equals(decoded)
          || containsDisallowedDecodedChar(decoded)) {
        return null;
      }
      decodedSegments.add(decoded);
    }
    return decodedSegments;
  }

  @Nullable
  private static String decodeSegment(String rawSegment) {
    try {
      String path = URI.create("/" + rawSegment).getPath();
      if (path == null || !path.startsWith("/")) {
        return null;
      }
      return path.substring(1);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private static boolean containsDisallowedDecodedChar(String decoded) {
    for (int i = 0; i < decoded.length(); i++) {
      char c = decoded.charAt(i);
      if (c == '/' || c == '\\' || Character.isISOControl(c)) {
        return true;
      }
    }
    return false;
  }

  private static boolean checkNamespace(NamespaceRule rule, @Nullable String namespace,
      ProgramRunId tokenRunId) {
    switch (rule) {
      case NONE:
        return true;
      case RUN:
        return tokenRunId.getNamespace().equals(namespace);
      case RUN_OR_SYSTEM:
        return tokenRunId.getNamespace().equals(namespace)
            || NamespaceId.SYSTEM.getNamespace().equals(namespace);
      default:
        return false;
    }
  }

  private static boolean checkApplication(ApplicationRule rule, @Nullable String app,
      ProgramRunId tokenRunId) {
    switch (rule) {
      case NONE:
        return true;
      case RUN:
        return tokenRunId.getApplication().equals(app);
      default:
        return false;
    }
  }

  private static RuntimeScopes.ScopeMatch checkScope(ScopeKind scopeKind,
      Map<String, String> captures, RuntimeScopes scopes) {
    switch (scopeKind) {
      case SECRET:
        return scopes.checkSecret(captures.get("name"));
      case CONNECTION:
        return scopes.checkConnection(captures.get("connection"));
      case OAUTH:
        return scopes.checkOauth(captures.get("provider"), captures.get("credential"));
      default:
        return RuntimeScopes.ScopeMatch.ALLOWED;
    }
  }

  private static boolean isWorkflowLocalDataset(@Nullable String datasetName,
      ProgramRunId tokenRunId) {
    if (datasetName == null) {
      return false;
    }
    String suffix = "." + tokenRunId.getRun();
    return datasetName.length() > suffix.length() && datasetName.endsWith(suffix);
  }

  private static boolean isFileSetType(String typeName) {
    return FileSet.TYPE.equals(typeName) || FileSet.class.getName().equals(typeName);
  }

  private static boolean hasForbiddenFileSetProperty(Map<String, String> properties) {
    for (String key : properties.keySet()) {
      if (key == null) {
        return true;
      }
      for (String suffix : FORBIDDEN_FILESET_PROPERTY_SUFFIXES) {
        if (key.endsWith(suffix)) {
          return true;
        }
      }
    }
    return false;
  }

  private static String extractLogPath(String servicePath) {
    try {
      URI uri = URI.create(servicePath);
      if (uri.getScheme() == null && uri.getRawAuthority() == null && uri.getPath() != null) {
        return sanitizeForLog(uri.getPath());
      }
    } catch (IllegalArgumentException ignored) {
      // Fall back to stripping query and fragment from the raw servicePath.
    }
    return sanitizeForLog(stripQueryAndFragment(servicePath));
  }

  private static String stripQueryAndFragment(String path) {
    int end = path.length();
    int queryIdx = path.indexOf('?');
    if (queryIdx >= 0 && queryIdx < end) {
      end = queryIdx;
    }
    int fragIdx = path.indexOf('#');
    if (fragIdx >= 0 && fragIdx < end) {
      end = fragIdx;
    }
    return path.substring(0, end);
  }

  private static String sanitizeForLog(String value) {
    StringBuilder sb = null;
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (Character.isISOControl(c)) {
        if (sb == null) {
          sb = new StringBuilder(value.length());
          sb.append(value, 0, i);
        }
        sb.append('?');
      } else if (sb != null) {
        sb.append(c);
      }
    }
    return sb == null ? value : sb.toString();
  }

  private static List<Rule> loadBundledRules() {
    InputStream stream = RuntimeCallbackAllowlist.class.getClassLoader()
        .getResourceAsStream(BUNDLED_ALLOWLIST_RESOURCE);
    if (stream == null) {
      throw new IllegalStateException(
          "Missing bundled runtime callback allowlist resource: " + BUNDLED_ALLOWLIST_RESOURCE);
    }
    try (Reader reader = new InputStreamReader(stream, StandardCharsets.UTF_8)) {
      return parseRules(reader, BUNDLED_ALLOWLIST_RESOURCE);
    } catch (IOException e) {
      throw new IllegalStateException(
          "Failed to read bundled runtime callback allowlist resource: "
              + BUNDLED_ALLOWLIST_RESOURCE, e);
    }
  }

  private static List<Rule> parseRules(Reader reader, String source) {
    JsonElement root;
    try {
      root = new JsonParser().parse(reader);
    } catch (JsonParseException e) {
      throw new IllegalArgumentException(
          "Invalid JSON in runtime callback allowlist (" + source + ")", e);
    }
    if (root == null || !root.isJsonArray()) {
      throw new IllegalArgumentException(
          "Runtime callback allowlist (" + source + ") must be a JSON array");
    }
    JsonArray array = root.getAsJsonArray();
    List<Rule> rules = new ArrayList<>(array.size());
    for (int i = 0; i < array.size(); i++) {
      JsonElement element = array.get(i);
      if (element == null || !element.isJsonObject()) {
        throw new IllegalArgumentException(
            "Allowlist rule at index " + i + " (" + source + ") must be a JSON object");
      }
      rules.add(parseRule(element.getAsJsonObject(), i, source));
    }
    return rules;
  }

  private static Rule parseRule(JsonObject obj, int index, String source) {
    for (Map.Entry<String, JsonElement> entry : obj.entrySet()) {
      if (!ALLOWED_RULE_KEYS.contains(entry.getKey())) {
        throw new IllegalArgumentException(
            "Unknown field '" + entry.getKey() + "' in allowlist rule at index " + index
                + " (" + source + ")");
      }
    }
    String pathTemplate = requireString(obj, "pathTemplate", index, source);
    List<TemplateSegment> segments = parseTemplate(pathTemplate, index, source);
    Set<String> captureNames = new HashSet<>();
    for (TemplateSegment segment : segments) {
      if (segment.captureName != null) {
        captureNames.add(segment.captureName);
      }
    }

    if (captureNames.contains("namespace") && !obj.has("namespaceRule")) {
      throw new IllegalArgumentException(
          "Allowlist rule at index " + index + " (" + source + ") captures {namespace} and must "
              + "explicitly specify namespaceRule");
    }
    NamespaceRule namespaceRule = parseEnum(
        obj, "namespaceRule", NamespaceRule.class, NamespaceRule.NONE, index, source);
    if (namespaceRule != NamespaceRule.NONE && !captureNames.contains("namespace")) {
      throw new IllegalArgumentException(
          "Allowlist rule at index " + index + " (" + source + ") requires {namespace} in "
              + "pathTemplate for namespaceRule=" + namespaceRule);
    }

    if (captureNames.contains("app") && !obj.has("applicationRule")) {
      throw new IllegalArgumentException(
          "Allowlist rule at index " + index + " (" + source + ") captures {app} and must "
              + "explicitly specify applicationRule");
    }
    ApplicationRule applicationRule = parseEnum(
        obj, "applicationRule", ApplicationRule.class, ApplicationRule.NONE, index, source);
    if (applicationRule != ApplicationRule.NONE && !captureNames.contains("app")) {
      throw new IllegalArgumentException(
          "Allowlist rule at index " + index + " (" + source + ") requires {app} in "
              + "pathTemplate for applicationRule=" + applicationRule);
    }

    BodyRule bodyRule = parseEnum(
        obj, "bodyRule", BodyRule.class, BodyRule.NONE, index, source);
    if (bodyRule != BodyRule.NONE && !captureNames.contains("name")) {
      throw new IllegalArgumentException(
          "Allowlist rule at index " + index + " (" + source + ") requires {name} in "
              + "pathTemplate for bodyRule=" + bodyRule);
    }

    ScopeKind scopeKind = parseEnum(
        obj, "scopeKind", ScopeKind.class, ScopeKind.NONE, index, source);
    if (bodyRule != BodyRule.NONE && scopeKind != ScopeKind.NONE) {
      throw new IllegalArgumentException(
          "Allowlist rule at index " + index + " (" + source + ") cannot combine bodyRule="
              + bodyRule + " with scopeKind=" + scopeKind);
    }
    if (!captureNames.containsAll(scopeKind.requiredCaptures)) {
      throw new IllegalArgumentException(
          "Allowlist rule at index " + index + " (" + source + ") requires "
              + scopeKind.requiredCaptures + " in pathTemplate for scopeKind=" + scopeKind);
    }

    Set<String> methods = parseMethods(obj, index, source);
    validateBodyRuleMethods(bodyRule, methods, index, source);

    String service = requireString(obj, "service", index, source);
    return new Rule(service, methods, pathTemplate, segments, namespaceRule, applicationRule,
        bodyRule, scopeKind);
  }

  private static void validateBodyRuleMethods(BodyRule bodyRule, Set<String> methods, int index,
      String source) {
    if (bodyRule == BodyRule.DATASET_PUT) {
      for (String method : methods) {
        if (!BODY_METHODS.contains(method)) {
          throw new IllegalArgumentException(
              "Allowlist rule at index " + index + " (" + source + ") with bodyRule=DATASET_PUT "
                  + "cannot be paired with method '" + method + "'");
        }
      }
    } else if (bodyRule == BodyRule.DATASET_DELETE) {
      for (String method : methods) {
        if (!"DELETE".equals(method)) {
          throw new IllegalArgumentException(
              "Allowlist rule at index " + index + " (" + source + ") with bodyRule=DATASET_DELETE "
                  + "can only be paired with DELETE, found '" + method + "'");
        }
      }
    }
  }

  private static String requireString(JsonObject obj, String field, int index, String source) {
    JsonElement element = obj.get(field);
    if (element == null || !element.isJsonPrimitive() || !element.getAsJsonPrimitive().isString()) {
      throw new IllegalArgumentException(
          "Missing or non-string field '" + field + "' in allowlist rule at index " + index
              + " (" + source + ")");
    }
    String value = element.getAsString();
    if (value.isEmpty()) {
      throw new IllegalArgumentException(
          "Empty field '" + field + "' in allowlist rule at index " + index + " (" + source + ")");
    }
    return value;
  }

  private static Set<String> parseMethods(JsonObject obj, int index, String source) {
    JsonElement element = obj.get("methods");
    if (element == null || !element.isJsonArray()) {
      throw new IllegalArgumentException(
          "Missing or non-array field 'methods' in allowlist rule at index " + index
              + " (" + source + ")");
    }
    JsonArray array = element.getAsJsonArray();
    if (array.size() == 0) {
      throw new IllegalArgumentException(
          "Empty 'methods' array in allowlist rule at index " + index + " (" + source + ")");
    }
    Set<String> methods = new HashSet<>();
    for (JsonElement methodEl : array) {
      if (methodEl == null || !methodEl.isJsonPrimitive()
          || !methodEl.getAsJsonPrimitive().isString()) {
        throw new IllegalArgumentException(
            "Non-string method in allowlist rule at index " + index + " (" + source + ")");
      }
      String method = methodEl.getAsString().trim().toUpperCase(Locale.ROOT);
      if (!SUPPORTED_METHODS.contains(method)) {
        throw new IllegalArgumentException(
            "Unsupported method '" + method + "' in allowlist rule at index " + index
                + " (" + source + ")");
      }
      methods.add(method);
    }
    return Collections.unmodifiableSet(methods);
  }

  private static List<TemplateSegment> parseTemplate(String pathTemplate, int index,
      String source) {
    if (!pathTemplate.startsWith("/") || pathTemplate.length() == 1 || pathTemplate.endsWith("/")) {
      throw new IllegalArgumentException(
          "Invalid pathTemplate '" + pathTemplate + "' in allowlist rule at index " + index
              + " (" + source + ")");
    }
    String[] parts = pathTemplate.substring(1).split("/", -1);
    List<TemplateSegment> segments = new ArrayList<>(parts.length);
    Set<String> seenCaptures = new HashSet<>();
    for (String part : parts) {
      if (part.isEmpty() || ".".equals(part) || "..".equals(part)) {
        throw new IllegalArgumentException(
            "Invalid segment '" + part + "' in pathTemplate '" + pathTemplate
                + "' at index " + index + " (" + source + ")");
      }
      if (part.startsWith("{") && part.endsWith("}")) {
        String captureName = part.substring(1, part.length() - 1);
        if (captureName.isEmpty() || captureName.indexOf('{') >= 0 || captureName.indexOf('}') >= 0) {
          throw new IllegalArgumentException(
              "Invalid capture '" + part + "' in pathTemplate '" + pathTemplate
                  + "' at index " + index + " (" + source + ")");
        }
        if (!seenCaptures.add(captureName)) {
          throw new IllegalArgumentException(
              "Duplicate capture '{" + captureName + "}' in pathTemplate '" + pathTemplate
                  + "' at index " + index + " (" + source + ")");
        }
        segments.add(TemplateSegment.capture(captureName));
      } else if (part.indexOf('{') >= 0 || part.indexOf('}') >= 0) {
        throw new IllegalArgumentException(
            "Malformed brace in segment '" + part + "' of pathTemplate '" + pathTemplate
                + "' at index " + index + " (" + source + ")");
      } else {
        segments.add(TemplateSegment.literal(part));
      }
    }
    return Collections.unmodifiableList(segments);
  }

  private static <E extends Enum<E>> E parseEnum(JsonObject obj, String field, Class<E> enumType,
      E defaultValue, int index, String source) {
    JsonElement element = obj.get(field);
    if (element == null) {
      return defaultValue;
    }
    if (!element.isJsonPrimitive() || !element.getAsJsonPrimitive().isString()) {
      throw new IllegalArgumentException(
          "Non-string enum field '" + field + "' in allowlist rule at index " + index
              + " (" + source + ")");
    }
    String rawValue = element.getAsString();
    try {
      return Enum.valueOf(enumType, rawValue);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Unknown " + field + " value '" + rawValue + "' in allowlist rule at index " + index
              + " (" + source + ")", e);
    }
  }

  /**
   * Result of matching an incoming callback request against the allowlist and per-run scopes.
   */
  static final class MatchResult {

    @Nullable
    private final RuntimeValidationFailure failure;
    @Nullable
    private final RuntimeScopeAdmission admission;
    private final String logPath;
    private final BodyRule bodyRule;
    private final Map<String, String> captures;

    private MatchResult(@Nullable RuntimeValidationFailure failure,
        @Nullable RuntimeScopeAdmission admission, String logPath, BodyRule bodyRule,
        Map<String, String> captures) {
      this.failure = failure;
      this.admission = admission;
      this.logPath = logPath;
      this.bodyRule = bodyRule;
      this.captures = captures;
    }

    static MatchResult allowed(String pathTemplate, BodyRule bodyRule,
        Map<String, String> captures, @Nullable RuntimeScopeAdmission admission) {
      return new MatchResult(null, admission, pathTemplate, bodyRule,
          Collections.unmodifiableMap(captures));
    }

    static MatchResult denied(RuntimeValidationFailure failure, String logPath) {
      return new MatchResult(Objects.requireNonNull(failure, "failure"), null, logPath,
          BodyRule.NONE, Collections.emptyMap());
    }

    boolean isAllowed() {
      return failure == null;
    }

    @Nullable
    RuntimeValidationFailure getFailure() {
      return failure;
    }

    @Nullable
    RuntimeScopeAdmission getAdmission() {
      return admission;
    }

    boolean requiresBodyCheck() {
      return failure == null && bodyRule == BodyRule.DATASET_PUT;
    }

    String getLogPath() {
      return logPath;
    }

    @Nullable
    String getCapture(String name) {
      return captures.get(name);
    }
  }

  private static final class Rule {

    private final String service;
    private final Set<String> methods;
    private final String pathTemplate;
    private final List<TemplateSegment> segments;
    private final NamespaceRule namespaceRule;
    private final ApplicationRule applicationRule;
    private final BodyRule bodyRule;
    private final ScopeKind scopeKind;

    private Rule(String service, Set<String> methods, String pathTemplate,
        List<TemplateSegment> segments, NamespaceRule namespaceRule,
        ApplicationRule applicationRule, BodyRule bodyRule, ScopeKind scopeKind) {
      this.service = service;
      this.methods = methods;
      this.pathTemplate = pathTemplate;
      this.segments = segments;
      this.namespaceRule = namespaceRule;
      this.applicationRule = applicationRule;
      this.bodyRule = bodyRule;
      this.scopeKind = scopeKind;
    }

    @Nullable
    private Map<String, String> matchSegments(List<String> decodedSegments) {
      if (decodedSegments.size() != segments.size()) {
        return null;
      }
      Map<String, String> captures = new HashMap<>();
      for (int i = 0; i < segments.size(); i++) {
        TemplateSegment templateSegment = segments.get(i);
        String actual = decodedSegments.get(i);
        if (templateSegment.captureName != null) {
          captures.put(templateSegment.captureName, actual);
        } else if (!templateSegment.literal.equals(actual)) {
          return null;
        }
      }
      return captures;
    }
  }

  private static final class TemplateSegment {

    @Nullable
    private final String literal;
    @Nullable
    private final String captureName;

    private TemplateSegment(@Nullable String literal, @Nullable String captureName) {
      this.literal = literal;
      this.captureName = captureName;
    }

    static TemplateSegment literal(String value) {
      return new TemplateSegment(value, null);
    }

    static TemplateSegment capture(String name) {
      return new TemplateSegment(null, name);
    }
  }
}
