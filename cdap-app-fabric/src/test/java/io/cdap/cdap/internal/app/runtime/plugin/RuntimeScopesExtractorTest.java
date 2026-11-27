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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.internal.app.DefaultApplicationSpecification;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeScopes;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TriggerMappedArguments;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link RuntimeScopesExtractor}.
 */
public class RuntimeScopesExtractorTest {

  private static final ProgramId WORKFLOW_ID =
      new NamespaceId("ns1").app("pipe1").workflow("DataPipelineWorkflow");
  private static final ArtifactId SYSTEM_ARTIFACT =
      new ArtifactId("cdap-data-pipeline", new ArtifactVersion("6.12.0"), ArtifactScope.SYSTEM);
  private static final ArtifactId USER_ARTIFACT =
      new ArtifactId("custom-app", new ArtifactVersion("1.0.0"), ArtifactScope.USER);
  private static final RuntimeScopesExtractor.ConnectionReader NOOP_READER =
      name -> Optional.empty();

  @Test
  public void testScanSourcesAndAppConfigForms() {
    // Non-macro-enabled plugin field is still scanned
    PluginPropertyField nonMacroField =
        new PluginPropertyField("plainField", "desc", "string", false, false);
    Plugin plugin = makePlugin("batchsource", "Src",
        Collections.singletonMap("plainField", nonMacroField),
        Collections.singletonMap("plainField", "${secure(from-non-macro-field)}"));

    // Reference in app config JSON string outside plugins + object key + array nesting
    String configJson = "{"
        + "\"nested\":{\"note\":\"token=${oauth(gh,cred-in-config)}\"},"
        + "\"${secure(from-config-key)}\":\"value\","
        + "\"items\":[123, true, null, \"${secure(from-config-array)}\"]"
        + "}";

    ApplicationSpecification spec = makeSpec(SYSTEM_ARTIFACT, configJson,
        Collections.singletonMap("p1", plugin));
    Map<String, String> userArgs = Collections.singletonMap("argKey", "${secure(from-user-arg)}");
    TriggerMappedArguments.Result triggerArgs = TriggerMappedArguments.Result.of(
        Collections.singletonMap("trigKey", "${secure(from-trigger-arg)}"));

    RuntimeScopes scopes = RuntimeScopesExtractor.extract(
        spec, WORKFLOW_ID, userArgs, triggerArgs, NOOP_READER);
    Assert.assertEquals(
        ImmutableSet.of("from-non-macro-field", "from-config-key", "from-config-array",
            "from-user-arg", "from-trigger-arg"),
        scopes.getSecrets());
    Assert.assertEquals(Collections.singleton("gh/cred-in-config"), scopes.getOauth());
    Assert.assertTrue(scopes.getConnections().isEmpty());
    Assert.assertFalse(scopes.hasExternalDynamicSecret());

    // Null, empty, JSON null, non-JSON (including leading 'null'), and deeply nested JSON config
    RuntimeScopes nullConfigScopes = RuntimeScopesExtractor.extract(
        makeSpec(SYSTEM_ARTIFACT, null, Collections.emptyMap()),
        WORKFLOW_ID, null, null, NOOP_READER);
    Assert.assertTrue(nullConfigScopes.getSecrets().isEmpty());
    Assert.assertFalse(nullConfigScopes.hasExternalDynamicSecret());

    RuntimeScopes emptyConfigScopes = RuntimeScopesExtractor.extract(
        makeSpec(SYSTEM_ARTIFACT, "", Collections.emptyMap()),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertTrue(emptyConfigScopes.getSecrets().isEmpty());

    RuntimeScopes jsonNullConfigScopes = RuntimeScopesExtractor.extract(
        makeSpec(SYSTEM_ARTIFACT, "null", Collections.emptyMap()),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertTrue(jsonNullConfigScopes.getSecrets().isEmpty());

    RuntimeScopes nonJsonLeadingNullScopes = RuntimeScopesExtractor.extract(
        makeSpec(SYSTEM_ARTIFACT, "null ${secure(from-leading-null)}", Collections.emptyMap()),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(Collections.singleton("from-leading-null"),
        nonJsonLeadingNullScopes.getSecrets());

    RuntimeScopes nonJsonConfigScopes = RuntimeScopesExtractor.extract(
        makeSpec(SYSTEM_ARTIFACT, "raw ${secure(from-raw-config)}", Collections.emptyMap()),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(Collections.singleton("from-raw-config"),
        nonJsonConfigScopes.getSecrets());

    // Malformed unicode escape throws NumberFormatException in JsonReader: falls back to raw scan
    RuntimeScopes badUnicodeEscapeScopes = RuntimeScopesExtractor.extract(
        makeSpec(SYSTEM_ARTIFACT,
            "{\"path\":\"C:\\users\\alice\",\"pw\":\"${secure(after-bad-escape)}\"}",
            Collections.emptyMap()),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(Collections.singleton("after-bad-escape"),
        badUnicodeEscapeScopes.getSecrets());
    Assert.assertFalse(badUnicodeEscapeScopes.hasExternalDynamicSecret());

    // Lenient JSON (unquoted key, single quotes, unicode escape) still decodes escapes
    RuntimeScopes lenientJsonScopes = RuntimeScopesExtractor.extract(
        makeSpec(SYSTEM_ARTIFACT, "{k: '\\u0024{secure(from-lenient-json)}'}",
            Collections.emptyMap()),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(Collections.singleton("from-lenient-json"),
        lenientJsonScopes.getSecrets());
    Assert.assertFalse(lenientJsonScopes.hasExternalDynamicSecret());

    // Deeply nested JSON with JSON-escaped macro delimiter must stream iteratively without SOE
    StringBuilder deepJson = new StringBuilder();
    int depth = 3000;
    for (int i = 0; i < depth; i++) {
      deepJson.append('[');
    }
    deepJson.append("\"\\u0024{secure(deep-escaped)}\"");
    for (int i = 0; i < depth; i++) {
      deepJson.append(']');
    }
    RuntimeScopes deepConfigScopes = RuntimeScopesExtractor.extract(
        makeSpec(SYSTEM_ARTIFACT, deepJson.toString(), Collections.emptyMap()),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(Collections.singleton("deep-escaped"), deepConfigScopes.getSecrets());
    Assert.assertFalse(deepConfigScopes.hasExternalDynamicSecret());
  }

  @Test
  public void testEscapedAndUnescapedForms() {
    // Escaped vs unescaped forms, including cases that uniquely require ESCAPING_ON:
    // - ${secure(my\)secret)} fails under ESCAPING_OFF, succeeds only under ESCAPING_ON
    // - ${secure(a\\b)} yields "a\b" under ESCAPING_ON and "a\\b" under ESCAPING_OFF
    // - ${secure(left-of-esc)} \${unclosed fails on right under ESCAPING_OFF before reaching left
    ApplicationSpecification spec = makeSpec(SYSTEM_ARTIFACT, null, ImmutableMap.of(
        "p1", makePlugin("batchsource", "S1", Collections.emptyMap(),
            ImmutableMap.of(
                "escapedAndUnescaped", "a=\\${secure(escaped-secret)} b=${secure(unescaped-secret)}",
                "escapedParen", "${secure(my\\)secret)}",
                "escapedBackslash", "${secure(a\\\\b)}",
                "escapedUnclosedOnRight", "${secure(left-of-esc)} \\${unclosed"))));

    RuntimeScopes scopes = RuntimeScopesExtractor.extract(
        spec, WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(),
        NOOP_READER);
    Assert.assertEquals(
        ImmutableSet.of("escaped-secret", "unescaped-secret", "my)secret",
            "a\\b", "a\\\\b", "left-of-esc"),
        scopes.getSecrets());
  }

  @Test
  public void testLookupSourcesAgreementDisagreementAndUnknown() {
    ApplicationSpecification spec = specWithSingleProp(
        "${secure(u-${userOnly})} "
            + "${secure(t-${triggerOnly})} "
            + "${secure(a-${agree})} "
            + "${secure(d-${disagree})} "
            + "${secure(x-${unknown})} "
            + "${secure(r-${outer})} "
            + "${secure(ru-${outerUnknown})}");

    Map<String, String> userArgs = ImmutableMap.<String, String>builder()
        .put("userOnly", "u1")
        .put("agree", "same")
        .put("disagree", "v1")
        .put("outer", "mid-${inner}")
        .put("inner", "end")
        .put("outerUnknown", "mid-${missingNested}")
        .build();
    TriggerMappedArguments.Result triggerArgs = TriggerMappedArguments.Result.of(
        ImmutableMap.of(
            "triggerOnly", "t1",
            "agree", "same",
            "disagree", "v2"));

    RuntimeScopes scopes = RuntimeScopesExtractor.extract(
        spec, WORKFLOW_ID, userArgs, triggerArgs, NOOP_READER);
    Assert.assertEquals(
        ImmutableSet.of("u-u1", "t-t1", "a-same", "d-*", "x-*", "r-mid-end", "ru-mid-*"),
        scopes.getSecrets());
    Assert.assertFalse(scopes.hasExternalDynamicSecret());

    // Cyclic lookups terminate cleanly via MacroParser's depth limit without throwing
    Map<String, String> cyclicArgs = ImmutableMap.of("cycA", "${cycB}", "cycB", "${cycA}");
    RuntimeScopes cyclicScopes = RuntimeScopesExtractor.extract(
        specWithSingleProp("${secure(${cycA})} ${secure(after-cycle)}"),
        WORKFLOW_ID, cyclicArgs, TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(Collections.singleton("after-cycle"), cyclicScopes.getSecrets());
    Assert.assertFalse(cyclicScopes.hasExternalDynamicSecret());
  }

  @Test
  public void testScopedOverrideShadowing() {
    ApplicationSpecification spec = specWithSingleProp(
        "${secure(s1-${k1})} ${secure(s2-${k2})} ${secure(s3-${k3})} "
            + "${secure(s4-${k4})} ${secure(s5-${k5})} ${secure(s6-${k6})} "
            + "${secure(b1-${kb1})} ${secure(b2-${kb2})}");

    Map<String, String> userArgs = ImmutableMap.<String, String>builder()
        .put("k1", "v1")
        .put("spark.*.k1", "override1")
        .put("k2", "v2")
        .put("action.nodeA.k2", "override2")
        .put("k3", "v3")
        .put("task.mapper.k3", "override3")
        .put("k4", "v4")
        .put("workflow.DataPipelineWorkflow.k4", "alreadyConsumedByTopLevel")
        .put("k5", "v5")
        .put("mapreduce.phase1.k5", "override5")
        .put("k6", "v6")
        .put("kb1", "vb1")
        .put("spark.kb1", "notScopedFormat")
        .put("kb2", "vb2")
        .put("spark..kb2", "emptyScopeTarget")
        .build();
    // Scoped override supplied via trigger-mapped arguments also shadows lookup
    TriggerMappedArguments.Result triggerArgs = TriggerMappedArguments.Result.of(
        Collections.singletonMap("spark.innerSp.k6", "overrideFromTrigger"));

    RuntimeScopes scopes = RuntimeScopesExtractor.extract(
        spec, WORKFLOW_ID, userArgs, triggerArgs, NOOP_READER);
    Assert.assertEquals(
        ImmutableSet.of("s1-*", "s2-*", "s3-*", "s4-v4", "s5-*", "s6-*", "b1-vb1", "b2-vb2"),
        scopes.getSecrets());
  }

  @Test
  public void testTopLevelProgramOwnScopeNotShadowed() {
    ProgramId topSpark = new NamespaceId("ns1").app("app1").spark("sp1");
    ApplicationSpecification spec = specWithSingleProp(
        "${secure(exact-${k1})} ${secure(wild-${k2})} ${secure(other-${k3})} "
            + "${secure(task-${k4})}");

    Map<String, String> userArgs = ImmutableMap.<String, String>builder()
        .put("k1", "v1")
        .put("spark.sp1.k1", "consumedExact")
        .put("k2", "v2")
        .put("spark.*.k2", "consumedWildcard")
        .put("k3", "v3")
        .put("spark.otherSpark.k3", "differentSparkScope")
        .put("k4", "v4")
        .put("task.mapper.k4", "taskOverride")
        .build();

    RuntimeScopes scopes = RuntimeScopesExtractor.extract(
        spec, topSpark, userArgs, TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(
        ImmutableSet.of("exact-v1", "wild-v2", "other-*", "task-*"),
        scopes.getSecrets());

    // Top-level MapReduce consumes mapreduce.<name>. and mapreduce.*., but not task.mapper.
    ProgramId topMr = new NamespaceId("ns1").app("app1").mr("mr1");
    Map<String, String> mrArgs = ImmutableMap.<String, String>builder()
        .put("k1", "v1")
        .put("mapreduce.mr1.k1", "consumedExact")
        .put("k2", "v2")
        .put("mapreduce.*.k2", "consumedWildcard")
        .put("k3", "v3")
        .put("mapreduce.otherMr.k3", "differentMr")
        .put("k4", "v4")
        .put("task.mapper.k4", "taskOverride")
        .build();
    RuntimeScopes mrScopes = RuntimeScopesExtractor.extract(
        spec, topMr, mrArgs, TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(
        ImmutableSet.of("exact-v1", "wild-v2", "other-*", "task-*"),
        mrScopes.getSecrets());
  }

  @Test
  public void testSkipNormalMacroEvaluationRecordsBothValueAndPattern() {
    ApplicationSpecification spec = specWithSingleProp(
        "${secure(pfx-${env})} ${secure(esc\\-${env})} ${conn(myconn)}");

    Map<String, String> userArgs = ImmutableMap.of(
        "env", "prod",
        "connKey", "s1",
        SystemArguments.SKIP_NORMAL_MACRO_EVALUATION, "true");

    RuntimeScopesExtractor.ConnectionReader reader = name -> {
      if ("myconn".equals(name)) {
        return Optional.of(Collections.singletonMap("secret", "${secure(c-${connKey})}"));
      }
      return Optional.empty();
    };

    RuntimeScopes scopes = RuntimeScopesExtractor.extract(
        spec, WORKFLOW_ID, userArgs, TriggerMappedArguments.Result.empty(), reader);
    Assert.assertEquals(
        ImmutableSet.of("pfx-prod", "pfx-*", "esc-prod", "esc-*",
            "esc\\-prod", "esc\\-*", "c-s1", "c-*"),
        scopes.getSecrets());
    Assert.assertEquals(Collections.singleton("myconn"), scopes.getConnections());
    Assert.assertFalse(scopes.hasExternalDynamicSecret());
  }

  @Test
  public void testSecretShapesAndOtherFunctions() {
    // Exact and pattern
    RuntimeScopes exactAndPattern = RuntimeScopesExtractor.extract(
        specWithSingleProp("${secure(exact-key)} ${secure(pre-${unknown}-suf)}"),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(ImmutableSet.of("exact-key", "pre-*-suf"), exactAndPattern.getSecrets());
    Assert.assertFalse(exactAndPattern.hasExternalDynamicSecret());

    // Only lookup markers -> dynamic
    RuntimeScopes onlyMarkers = RuntimeScopesExtractor.extract(
        specWithSingleProp("${secure(${a}${b})}"),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertTrue(onlyMarkers.getSecrets().isEmpty());
    Assert.assertTrue(onlyMarkers.hasExternalDynamicSecret());

    // Nested function result (alone and combined with lookup marker) -> dynamic
    RuntimeScopes nestedFunc = RuntimeScopesExtractor.extract(
        specWithSingleProp("${secure(prefix-${logicalStartTime(yyyy-MM-dd)})} "
            + "${secure(${unknown}-${logicalStartTime(yyyy)})}"),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertTrue(nestedFunc.getSecrets().isEmpty());
    Assert.assertTrue(nestedFunc.hasExternalDynamicSecret());

    // Empty secret/oauth arguments -> nothing recorded
    RuntimeScopes emptyArgs = RuntimeScopesExtractor.extract(
        specWithSingleProp("${secure()} ${oauth(p,)} ${oauth(,c)}"),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertTrue(emptyArgs.getSecrets().isEmpty());
    Assert.assertTrue(emptyArgs.getOauth().isEmpty());
    Assert.assertFalse(emptyArgs.hasExternalDynamicSecret());

    // logicalStartTime alone -> ignored
    RuntimeScopes logicalOnly = RuntimeScopesExtractor.extract(
        specWithSingleProp("${logicalStartTime(yyyy-MM-dd, 1h, UTC)}"),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertTrue(logicalOnly.getSecrets().isEmpty());
    Assert.assertTrue(logicalOnly.getOauth().isEmpty());
    Assert.assertTrue(logicalOnly.getConnections().isEmpty());
    Assert.assertFalse(logicalOnly.hasExternalDynamicSecret());
  }

  @Test
  public void testWrongArityAndFunctionNameWithMarker() {
    // Wrong arity without markers records nothing; macros to its left (evaluated after) still found
    RuntimeScopes wrongArityNoMarkers = RuntimeScopesExtractor.extract(
        specWithSingleProp("${secure(left-ok)} ${secure(a, b)} ${oauth(onlyone)} ${conn(c1, c2)}"),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(Collections.singleton("left-ok"), wrongArityNoMarkers.getSecrets());
    Assert.assertTrue(wrongArityNoMarkers.getOauth().isEmpty());
    Assert.assertTrue(wrongArityNoMarkers.getConnections().isEmpty());
    Assert.assertFalse(wrongArityNoMarkers.hasExternalDynamicSecret());

    // One unresolved lookup supplying both OAuth arguments -> */*
    RuntimeScopes oauthUnknownArity = RuntimeScopesExtractor.extract(
        specWithSingleProp("${oauth(${bothArgs})}"),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(Collections.singleton(RuntimeScopes.OAUTH_WILDCARD),
        oauthUnknownArity.getOauth());
    Assert.assertTrue(oauthUnknownArity.getConnections().isEmpty());
    Assert.assertFalse(oauthUnknownArity.hasExternalDynamicSecret());

    // Wrong arity with marker for secure -> dynamic
    RuntimeScopes secureWrongArityMarker = RuntimeScopesExtractor.extract(
        specWithSingleProp("${secure(a, ${extra})}"),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertTrue(secureWrongArityMarker.hasExternalDynamicSecret());

    // Wrong arity with marker for conn -> widens connections, oauth, and dynamic
    RuntimeScopes connWrongArityMarker = RuntimeScopesExtractor.extract(
        specWithSingleProp("${conn(a, ${extra})}"),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(Collections.singleton(RuntimeScopes.WILDCARD),
        connWrongArityMarker.getConnections());
    Assert.assertEquals(Collections.singleton(RuntimeScopes.OAUTH_WILDCARD),
        connWrongArityMarker.getOauth());
    Assert.assertTrue(connWrongArityMarker.hasExternalDynamicSecret());

    // Function name containing a lookup widens all three
    RuntimeScopes dynamicFuncName = RuntimeScopesExtractor.extract(
        specWithSingleProp("${${fn}(arg)}"),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(Collections.singleton(RuntimeScopes.WILDCARD),
        dynamicFuncName.getConnections());
    Assert.assertEquals(Collections.singleton(RuntimeScopes.OAUTH_WILDCARD),
        dynamicFuncName.getOauth());
    Assert.assertTrue(dynamicFuncName.hasExternalDynamicSecret());
  }

  @Test
  public void testLiteralWildcardSlashAndMarkerSanitization() {
    // Literal '*' in secure or oauth, or '/' in oauth, records nothing (even combined with
    // unresolved lookup markers); conn(*) records '_' (not wildcard)
    List<String> readNames = new ArrayList<>();
    RuntimeScopesExtractor.ConnectionReader reader = name -> {
      readNames.add(name);
      return Optional.empty();
    };
    RuntimeScopes literalScopes = RuntimeScopesExtractor.extract(
        specWithSingleProp("${secure(*)} ${secure(a*b)} ${secure(a*${k})} "
            + "${oauth(*,*)} ${oauth(a/b,c)} ${oauth(p,c/d)} ${oauth(p,*)} "
            + "${oauth(a*${k},c)} ${oauth(a/${k},c)} ${conn(*)}"),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(), reader);
    Assert.assertTrue(literalScopes.getSecrets().isEmpty());
    Assert.assertTrue(literalScopes.getOauth().isEmpty());
    Assert.assertEquals(Collections.singleton("_"), literalScopes.getConnections());
    Assert.assertFalse(literalScopes.allowsConnection("other_conn"));
    Assert.assertEquals(Collections.singletonList("*"), readNames);
    Assert.assertFalse(literalScopes.hasExternalDynamicSecret());

    // Private-use marker characters already in text (including between '$' and '{')
    // or in lookup return values are stripped before parsing
    char lookupMarker = RuntimeScopesExtractor.LOOKUP_MARKER;
    char functionMarker = RuntimeScopesExtractor.FUNCTION_MARKER;
    String textWithMarkers = "${secure(my" + lookupMarker + "secret)} "
        + "${secure(fn" + functionMarker + "secret)} ${secure(${fromArg})}";
    Map<String, String> userArgs = Collections.singletonMap(
        "fromArg", "val" + lookupMarker + functionMarker + "ok");
    RuntimeScopes sanitizedScopes = RuntimeScopesExtractor.extract(
        specWithSingleProp(textWithMarkers),
        WORKFLOW_ID, userArgs, TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(
        ImmutableSet.of("mysecret", "fnsecret", "valok"),
        sanitizedScopes.getSecrets());
    Assert.assertFalse(sanitizedScopes.hasExternalDynamicSecret());

    // Text whose only macro delimiter is split by a marker character still extracts it
    RuntimeScopes splitDelimScopes = RuntimeScopesExtractor.extract(
        specWithSingleProp("$" + lookupMarker + "{secure(split-delim)}"),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(Collections.singleton("split-delim"), splitDelimScopes.getSecrets());
    Assert.assertFalse(splitDelimScopes.hasExternalDynamicSecret());
  }

  @Test
  public void testOauthAndOauthAccessTokenPartialMarkers() {
    ApplicationSpecification spec = specWithSingleProp(
        "${oauth(github,cred1)} "
            + "${oauth(spaced, credSpace)} "
            + "${oauthAccessToken(${prov},cred2)} "
            + "${oauth(gitlab,${cred})} "
            + "${oauthAccessToken(${p2},${c2})}");

    RuntimeScopes scopes = RuntimeScopesExtractor.extract(
        spec, WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(),
        NOOP_READER);
    Assert.assertEquals(
        ImmutableSet.of("github/cred1", "spaced/ credSpace", "*/cred2", "gitlab/*", "*/*"),
        scopes.getOauth());
    Assert.assertFalse(scopes.hasExternalDynamicSecret());
  }

  @Test
  public void testConnectionsNestedReadsDedupAndCycles() throws UnsupportedEncodingException {
    // Same connection referenced by two plugins -> read once, contributes nested scopes
    Plugin p1 = makePlugin("batchsource", "S1", Collections.emptyMap(),
        Collections.singletonMap("c", "${conn(My Conn)}"));
    Plugin p2 = makePlugin("batchsink", "S2", Collections.emptyMap(),
        Collections.singletonMap("c", "${conn(My Conn)}"));
    ApplicationSpecification spec = makeSpec(SYSTEM_ARTIFACT, null,
        ImmutableMap.of("p1", p1, "p2", p2));

    AtomicInteger readCount = new AtomicInteger();
    RuntimeScopesExtractor.ConnectionReader reader = name -> {
      readCount.incrementAndGet();
      if ("My Conn".equals(name)) {
        return Optional.of(ImmutableMap.of(
            "password", "${secure(conn-secret)}",
            "token", "${oauthAccessToken(sf,cred-a)}",
            "next", "${conn(ConnB)}"));
      }
      if ("ConnB".equals(name)) {
        // Cycle back to My Conn and self-cycle to ConnB
        return Optional.of(ImmutableMap.of(
            "loopBack", "${conn(My Conn)}",
            "selfLoop", "${conn(ConnB)}",
            "nestedSecret", "${secure(from-b)}"));
      }
      return Optional.empty();
    };

    RuntimeScopes scopes = RuntimeScopesExtractor.extract(
        spec, WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(), reader);
    Assert.assertEquals(2, readCount.get());
    Assert.assertEquals(ImmutableSet.of("My_Conn", "ConnB"), scopes.getConnections());
    Assert.assertEquals(ImmutableSet.of("conn-secret", "from-b"), scopes.getSecrets());
    Assert.assertEquals(Collections.singleton("sf/cred-a"), scopes.getOauth());
    Assert.assertFalse(scopes.hasExternalDynamicSecret());

    // Space-only and leading/trailing space connection names: recorded and read untrimmed
    List<String> spaceReadNames = new ArrayList<>();
    RuntimeScopesExtractor.ConnectionReader spaceReader = name -> {
      spaceReadNames.add(name);
      return Optional.empty();
    };
    RuntimeScopes spaceScopes = RuntimeScopesExtractor.extract(
        specWithSingleProp("${conn(   )} ${conn(  my conn  )}"),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(), spaceReader);
    Assert.assertEquals(ImmutableSet.of("___", "__my_conn__"), spaceScopes.getConnections());
    Assert.assertEquals(ImmutableSet.of("   ", "  my conn  "), ImmutableSet.copyOf(spaceReadNames));
    Assert.assertTrue(spaceScopes.allowsConnection(
        URLEncoder.encode("   ", StandardCharsets.UTF_8.name())));
    Assert.assertTrue(spaceScopes.allowsConnection(
        URLEncoder.encode("  my conn  ", StandardCharsets.UTF_8.name())));
  }

  @Test
  public void testConnectionNotFoundReadFailureMarkerAndUnusableNames() {
    // Not found / 400 -> Optional.empty(): ID recorded, nothing widened
    RuntimeScopes notFoundScopes = RuntimeScopesExtractor.extract(
        specWithSingleProp("${conn(missing-conn)}"),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(),
        name -> Optional.empty());
    Assert.assertEquals(Collections.singleton("missing_conn"), notFoundScopes.getConnections());
    Assert.assertTrue(notFoundScopes.getOauth().isEmpty());
    Assert.assertFalse(notFoundScopes.hasExternalDynamicSecret());

    // Read failure (IOException, RuntimeException, null Optional) widens without stopping queue
    RuntimeScopes readFailureScopes = RuntimeScopesExtractor.extract(
        specWithSingleProp("${conn(healthy-conn)} ${conn(null-conn)} "
            + "${conn(runtime-fail-conn)} ${conn(flaky-conn)}"),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(),
        name -> {
          if ("flaky-conn".equals(name)) {
            throw new IOException("service unavailable");
          }
          if ("runtime-fail-conn".equals(name)) {
            throw new IllegalStateException("unexpected reader error");
          }
          if ("null-conn".equals(name)) {
            return null;
          }
          return Optional.of(Collections.singletonMap("s", "${secure(from-healthy)}"));
        });
    Assert.assertEquals(
        ImmutableSet.of("healthy_conn", "null_conn", "runtime_fail_conn", "flaky_conn",
            RuntimeScopes.WILDCARD),
        readFailureScopes.getConnections());
    Assert.assertEquals(Collections.singleton(RuntimeScopes.OAUTH_WILDCARD),
        readFailureScopes.getOauth());
    Assert.assertEquals(Collections.singleton("from-healthy"), readFailureScopes.getSecrets());
    Assert.assertTrue(readFailureScopes.hasExternalDynamicSecret());

    // Null Optional alone must also widen (must not be treated as 404)
    RuntimeScopes nullReturnScopes = RuntimeScopesExtractor.extract(
        specWithSingleProp("${conn(broken-reader-conn)}"),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(),
        name -> null);
    Assert.assertEquals(ImmutableSet.of("broken_reader_conn", RuntimeScopes.WILDCARD),
        nullReturnScopes.getConnections());
    Assert.assertEquals(Collections.singleton(RuntimeScopes.OAUTH_WILDCARD),
        nullReturnScopes.getOauth());
    Assert.assertTrue(nullReturnScopes.getSecrets().isEmpty());
    Assert.assertTrue(nullReturnScopes.hasExternalDynamicSecret());

    // Marker in connection name -> widens without calling reader
    AtomicInteger markerReadCalls = new AtomicInteger();
    RuntimeScopes markerConnScopes = RuntimeScopesExtractor.extract(
        specWithSingleProp("${conn(pre-${unknownConn})}"),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(),
        name -> {
          markerReadCalls.incrementAndGet();
          return Optional.empty();
        });
    Assert.assertEquals(0, markerReadCalls.get());
    Assert.assertEquals(Collections.singleton(RuntimeScopes.WILDCARD),
        markerConnScopes.getConnections());
    Assert.assertEquals(Collections.singleton(RuntimeScopes.OAUTH_WILDCARD),
        markerConnScopes.getOauth());
    Assert.assertTrue(markerConnScopes.hasExternalDynamicSecret());

    // Unusable names (empty, '.', '..', '/', ID-rule-rejected chars/whitespace) -> not recorded/read
    AtomicInteger unusableReadCalls = new AtomicInteger();
    RuntimeScopes unusableScopes = RuntimeScopesExtractor.extract(
        specWithSingleProp("${conn()} ${conn(.)} ${conn(..)} ${conn(a/b)} "
            + "${conn(a$b)} ${conn(\t)}"),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(),
        name -> {
          unusableReadCalls.incrementAndGet();
          return Optional.empty();
        });
    Assert.assertEquals(0, unusableReadCalls.get());
    Assert.assertTrue(unusableScopes.getConnections().isEmpty());
    Assert.assertFalse(unusableScopes.hasExternalDynamicSecret());
  }

  @Test
  public void testParseErrorKeepsEarlierResults() {
    // MacroParser evaluates right-to-left: ${secure(ok)} runs before malformed ${bad fails
    RuntimeScopes scopes = RuntimeScopesExtractor.extract(
        specWithSingleProp("${bad ${secure(ok)}"),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(Collections.singleton("ok"), scopes.getSecrets());
    Assert.assertFalse(scopes.hasExternalDynamicSecret());
  }

  @Test
  public void testDynamicPluginTypesAndUserArtifact() {
    for (String dynamicType : ImmutableSet.of("action", "postaction", "condition",
        "sparkprogram")) {
      Plugin p = makePlugin(dynamicType, "DynPlugin", Collections.emptyMap(),
          Collections.emptyMap());
      ApplicationSpecification spec = makeSpec(SYSTEM_ARTIFACT, null,
          Collections.singletonMap("dyn", p));
      RuntimeScopes scopes = RuntimeScopesExtractor.extract(
          spec, WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(),
          NOOP_READER);
      Assert.assertTrue("Expected dynamic secret flag for plugin type " + dynamicType,
          scopes.hasExternalDynamicSecret());
    }

    // User-scoped application artifact -> dynamic
    RuntimeScopes userArtifactScopes = RuntimeScopesExtractor.extract(
        makeSpec(USER_ARTIFACT, null, Collections.emptyMap()),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertTrue(userArtifactScopes.hasExternalDynamicSecret());
  }

  @Test
  public void testUserScopedPluginDynamicSecretFlag() {
    RuntimeScopes expectedDynamicOnly = new RuntimeScopes(
        Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), true);
    RuntimeScopes expectedEmpty = new RuntimeScopes(
        Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), false);

    // USER-scoped batchsource -> hasExternalDynamicSecret true
    Plugin userSource = makePlugin(USER_ARTIFACT, "batchsource", "CustomSrc",
        Collections.emptyMap(), Collections.emptyMap());
    RuntimeScopes userSourceScopes = RuntimeScopesExtractor.extract(
        makeSpec(SYSTEM_ARTIFACT, null, Collections.singletonMap("src", userSource)),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(expectedDynamicOnly, userSourceScopes);

    // SYSTEM-scoped batchsource -> hasExternalDynamicSecret false
    Plugin systemSource = makePlugin(SYSTEM_ARTIFACT, "batchsource", "SysSrc",
        Collections.emptyMap(), Collections.emptyMap());
    RuntimeScopes systemSourceScopes = RuntimeScopesExtractor.extract(
        makeSpec(SYSTEM_ARTIFACT, null, Collections.singletonMap("src", systemSource)),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(expectedEmpty, systemSourceScopes);

    // USER-scoped "jdbc" plugin only -> exempt, hasExternalDynamicSecret false
    Plugin userJdbc = makePlugin(USER_ARTIFACT, "jdbc", "mysql",
        Collections.emptyMap(), Collections.emptyMap());
    RuntimeScopes userJdbcScopes = RuntimeScopesExtractor.extract(
        makeSpec(SYSTEM_ARTIFACT, null, Collections.singletonMap("driver", userJdbc)),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(expectedEmpty, userJdbcScopes);

    // Plugin with null artifact id -> no exception, hasExternalDynamicSecret false
    Plugin nullArtifactPlugin = makePlugin(null, "batchsource", "NullArt",
        Collections.emptyMap(), Collections.emptyMap());
    RuntimeScopes nullArtifactScopes = RuntimeScopesExtractor.extract(
        makeSpec(SYSTEM_ARTIFACT, null, Collections.singletonMap("nullArt", nullArtifactPlugin)),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(expectedEmpty, nullArtifactScopes);

    // USER-scoped plugin with null PluginClass -> no exception, fail-safe dynamic true
    Plugin userNullClassPlugin = new Plugin(Collections.emptyList(), USER_ARTIFACT, null,
        PluginProperties.builder().build());
    RuntimeScopes userNullClassScopes = RuntimeScopesExtractor.extract(
        makeSpec(SYSTEM_ARTIFACT, null, Collections.singletonMap("nullCls", userNullClassPlugin)),
        WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(expectedDynamicOnly, userNullClassScopes);
  }

  @Test
  public void testArgumentSettingStagesWidenDefaultedLookups() {
    Map<String, String> connLaunchArgs = Collections.singletonMap("k", "dev_conn");
    Plugin connSource = makePlugin("batchsource", "Src", Collections.emptyMap(),
        Collections.singletonMap("conn", "${conn(${k})}"));
    Plugin actionPlugin = makePlugin("action", "ArgSetter", Collections.emptyMap(),
        Collections.emptyMap());

    // 1. k=dev_conn at launch + action plugin + ${conn(${k})} -> dev_conn, "*", and oauth "*/*"
    RuntimeScopes actionConnScopes = RuntimeScopesExtractor.extract(
        makeSpec(SYSTEM_ARTIFACT, null,
            ImmutableMap.of("src", connSource, "act", actionPlugin)),
        WORKFLOW_ID, connLaunchArgs, TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(ImmutableSet.of("dev_conn", RuntimeScopes.WILDCARD),
        actionConnScopes.getConnections());
    Assert.assertEquals(Collections.singleton(RuntimeScopes.OAUTH_WILDCARD),
        actionConnScopes.getOauth());
    Assert.assertTrue(actionConnScopes.hasExternalDynamicSecret());

    // 2. c=dev at launch + action plugin + ${oauth(p,${c})} & ${oauthAccessToken(p2,${c})}
    Plugin oauthSource = makePlugin("batchsource", "OAuthSrc", Collections.emptyMap(),
        ImmutableMap.of("o1", "${oauth(p,${c})}", "o2", "${oauthAccessToken(p2,${c})}"));
    RuntimeScopes actionOauthScopes = RuntimeScopesExtractor.extract(
        makeSpec(SYSTEM_ARTIFACT, null,
            ImmutableMap.of("src", oauthSource, "act", actionPlugin)),
        WORKFLOW_ID, Collections.singletonMap("c", "dev"),
        TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(ImmutableSet.of("p/dev", "p/*", "p2/dev", "p2/*"),
        actionOauthScopes.getOauth());
    Assert.assertTrue(actionOauthScopes.getConnections().isEmpty());

    // 3. action plugin + ${conn(fixed)} -> connections strictly {fixed}, no "*"; nested secret
    //    lookup inside connection properties records both resolved value and wildcard pattern.
    Plugin fixedConnSource = makePlugin("batchsource", "FixedSrc", Collections.emptyMap(),
        Collections.singletonMap("conn", "${conn(fixed)}"));
    RuntimeScopesExtractor.ConnectionReader fixedReader = name -> "fixed".equals(name)
        ? Optional.of(Collections.singletonMap("pwd", "${secure(c-${k})}"))
        : Optional.empty();
    RuntimeScopes fixedConnScopes = RuntimeScopesExtractor.extract(
        makeSpec(SYSTEM_ARTIFACT, null,
            ImmutableMap.of("src", fixedConnSource, "act", actionPlugin)),
        WORKFLOW_ID, connLaunchArgs, TriggerMappedArguments.Result.empty(), fixedReader);
    Assert.assertEquals(Collections.singleton("fixed"), fixedConnScopes.getConnections());
    Assert.assertTrue(fixedConnScopes.getOauth().isEmpty());
    Assert.assertEquals(ImmutableSet.of("c-dev_conn", "c-*"), fixedConnScopes.getSecrets());
    Assert.assertTrue(fixedConnScopes.hasExternalDynamicSecret());

    // 4. Same inputs with no argument-setting stage -> unchanged: exactly {dev_conn}
    RuntimeScopes plainScopes = RuntimeScopesExtractor.extract(
        makeSpec(SYSTEM_ARTIFACT, null, Collections.singletonMap("src", connSource)),
        WORKFLOW_ID, connLaunchArgs, TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(Collections.singleton("dev_conn"), plainScopes.getConnections());
    Assert.assertTrue(plainScopes.getOauth().isEmpty());
    Assert.assertFalse(plainScopes.hasExternalDynamicSecret());

    // 5. Only a postaction plugin + ${conn(${k})} with k defaulted -> connections unchanged,
    //    hasExternalDynamicSecret still true.
    Plugin postActionPlugin = makePlugin("postaction", "EmailAlert", Collections.emptyMap(),
        Collections.emptyMap());
    RuntimeScopes postActionScopes = RuntimeScopesExtractor.extract(
        makeSpec(SYSTEM_ARTIFACT, null,
            ImmutableMap.of("src", connSource, "post", postActionPlugin)),
        WORKFLOW_ID, connLaunchArgs, TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(Collections.singleton("dev_conn"), postActionScopes.getConnections());
    Assert.assertTrue(postActionScopes.getOauth().isEmpty());
    Assert.assertTrue(postActionScopes.hasExternalDynamicSecret());

    // 6. USER-scoped batchsource adds "*"; USER-scoped "jdbc" only does not.
    Plugin userConnSource = makePlugin(USER_ARTIFACT, "batchsource", "UserSrc",
        Collections.emptyMap(), Collections.singletonMap("conn", "${conn(${k})}"));
    RuntimeScopes userSrcScopes = RuntimeScopesExtractor.extract(
        makeSpec(SYSTEM_ARTIFACT, null, Collections.singletonMap("src", userConnSource)),
        WORKFLOW_ID, connLaunchArgs, TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(ImmutableSet.of("dev_conn", RuntimeScopes.WILDCARD),
        userSrcScopes.getConnections());
    Assert.assertEquals(Collections.singleton(RuntimeScopes.OAUTH_WILDCARD),
        userSrcScopes.getOauth());
    Assert.assertTrue(userSrcScopes.hasExternalDynamicSecret());

    Plugin userJdbc = makePlugin(USER_ARTIFACT, "jdbc", "mysql",
        Collections.emptyMap(), Collections.emptyMap());
    RuntimeScopes userJdbcScopes = RuntimeScopesExtractor.extract(
        makeSpec(SYSTEM_ARTIFACT, null,
            ImmutableMap.of("src", connSource, "driver", userJdbc)),
        WORKFLOW_ID, connLaunchArgs, TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(Collections.singleton("dev_conn"), userJdbcScopes.getConnections());
    Assert.assertTrue(userJdbcScopes.getOauth().isEmpty());
    Assert.assertFalse(userJdbcScopes.hasExternalDynamicSecret());

    // 7. condition, sparkprogram, and USER-scoped app artifact also widen defaulted ${conn(${k})}.
    for (String argSettingType : ImmutableSet.of("condition", "sparkprogram")) {
      Plugin stage = makePlugin(argSettingType, "Stage", Collections.emptyMap(),
          Collections.emptyMap());
      RuntimeScopes stageScopes = RuntimeScopesExtractor.extract(
          makeSpec(SYSTEM_ARTIFACT, null, ImmutableMap.of("src", connSource, "stage", stage)),
          WORKFLOW_ID, connLaunchArgs, TriggerMappedArguments.Result.empty(), NOOP_READER);
      Assert.assertEquals("Expected wildcard connection for plugin type " + argSettingType,
          ImmutableSet.of("dev_conn", RuntimeScopes.WILDCARD), stageScopes.getConnections());
      Assert.assertEquals(Collections.singleton(RuntimeScopes.OAUTH_WILDCARD),
          stageScopes.getOauth());
    }

    RuntimeScopes userAppScopes = RuntimeScopesExtractor.extract(
        makeSpec(USER_ARTIFACT, null, Collections.singletonMap("src", connSource)),
        WORKFLOW_ID, connLaunchArgs, TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(ImmutableSet.of("dev_conn", RuntimeScopes.WILDCARD),
        userAppScopes.getConnections());
    Assert.assertEquals(Collections.singleton(RuntimeScopes.OAUTH_WILDCARD),
        userAppScopes.getOauth());
  }

  @Test
  public void testUnreadableTriggerAndUnexpectedExceptionWidenAll() {
    ApplicationSpecification spec = specWithSingleProp("${secure(s1)}");

    RuntimeScopes unreadableTrigger = RuntimeScopesExtractor.extract(
        spec, WORKFLOW_ID, Collections.emptyMap(),
        TriggerMappedArguments.Result.unreadable(), NOOP_READER);
    Assert.assertEquals(RuntimeScopes.wildcardAll(), unreadableTrigger);

    // Unexpected exception or StackOverflowError from ApplicationSpecification -> wildcardAll
    ApplicationSpecification brokenSpec = Mockito.mock(ApplicationSpecification.class);
    Mockito.when(brokenSpec.getArtifactId()).thenReturn(SYSTEM_ARTIFACT);
    Mockito.when(brokenSpec.getPlugins()).thenThrow(new RuntimeException("boom"));
    RuntimeScopes onException = RuntimeScopesExtractor.extract(
        brokenSpec, WORKFLOW_ID, Collections.emptyMap(),
        TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(RuntimeScopes.wildcardAll(), onException);

    ApplicationSpecification soeSpec = Mockito.mock(ApplicationSpecification.class);
    Mockito.when(soeSpec.getArtifactId()).thenReturn(SYSTEM_ARTIFACT);
    Mockito.when(soeSpec.getPlugins()).thenThrow(new StackOverflowError("deep"));
    RuntimeScopes onSoe = RuntimeScopesExtractor.extract(
        soeSpec, WORKFLOW_ID, Collections.emptyMap(),
        TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(RuntimeScopes.wildcardAll(), onSoe);
  }

  @Test
  public void testMacroExpansionBudgetWidensWithoutBlowupAndAllowsLargeLiterals() {
    // 1. Exponential substitution-count chain (a0="", a1..a10 each repeating ${a_{i-1}} 6 times)
    //    hits the substitution budget quickly and widens to wildcardAll().
    Map<String, String> cpuChainArgs = new HashMap<>();
    cpuChainArgs.put("a0", "");
    for (int i = 1; i <= 10; i++) {
      cpuChainArgs.put("a" + i, Strings.repeat("${a" + (i - 1) + "}", 6));
    }
    RuntimeScopes cpuChainScopes = RuntimeScopesExtractor.extract(
        specWithSingleProp("${a10}"), WORKFLOW_ID, cpuChainArgs,
        TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(RuntimeScopes.wildcardAll(), cpuChainScopes);

    // 2. Exponential string-growth chain (16-char a0, k=8, a0..a9) widens to wildcardAll().
    Map<String, String> memChainArgs = new HashMap<>();
    memChainArgs.put("a0", "0123456789abcdef");
    for (int i = 1; i <= 9; i++) {
      memChainArgs.put("a" + i, Strings.repeat("${a" + (i - 1) + "}", 8));
    }
    RuntimeScopes memChainScopes = RuntimeScopesExtractor.extract(
        specWithSingleProp("${a9}"), WORKFLOW_ID, memChainArgs,
        TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(RuntimeScopes.wildcardAll(), memChainScopes);

    // 3. Low-substitution-count expanded-length growth (1 and 8 substitutions << 10_000)
    //    specifically pins maxExpandedGrowth independently of maxSubstitutions.
    Assert.assertEquals(RuntimeScopes.wildcardAll(), RuntimeScopesExtractor.extract(
        specWithSingleProp("${big}"), WORKFLOW_ID,
        Collections.singletonMap("big", Strings.repeat("a", 70_000)),
        TriggerMappedArguments.Result.empty(), NOOP_READER));
    Assert.assertEquals(RuntimeScopes.wildcardAll(), RuntimeScopesExtractor.extract(
        specWithSingleProp("${a1}"), WORKFLOW_ID,
        ImmutableMap.of("a0", Strings.repeat("a", 10_000), "a1", Strings.repeat("${a0}", 7)),
        TriggerMappedArguments.Result.empty(), NOOP_READER));

    // 4. Repeated-substitution copy amplification (60 KiB a0 + 5000 empty ${a1} macros) hits
    //    cumulative copied-chars budget quickly and widens to wildcardAll().
    Assert.assertEquals(RuntimeScopes.wildcardAll(), RuntimeScopesExtractor.extract(
        specWithSingleProp(Strings.repeat("${a1}", 5_000) + "${a0}"), WORKFLOW_ID,
        ImmutableMap.of("a0", Strings.repeat("a", 60_000), "a1", ""),
        TriggerMappedArguments.Result.empty(), NOOP_READER));

    // 5. Many escaped "${" markers complete linearly without widening unrelated exact scopes.
    Plugin escapedPlugin = makePlugin("batchsource", "Src", Collections.emptyMap(),
        ImmutableMap.of("escaped", Strings.repeat("\\${", 50_000), "sec", "${secure(s1)}"));
    Assert.assertEquals(
        new RuntimeScopes(Collections.emptySet(), Collections.emptySet(),
            Collections.singleton("s1"), false),
        RuntimeScopesExtractor.extract(
            makeSpec(SYSTEM_ARTIFACT, null, Collections.singletonMap("src", escapedPlugin)),
            WORKFLOW_ID, Collections.emptyMap(), TriggerMappedArguments.Result.empty(),
            NOOP_READER));

    // 6. Budget overflow inside appSpec.getConfiguration() JSON and inside connection properties
    //    must propagate out of scanJsonTokens / drainConnectionQueue and widen to wildcardAll().
    ApplicationSpecification jsonOverflowSpec = makeSpec(
        SYSTEM_ARTIFACT, "{\"k\":\"${big}\"}", Collections.emptyMap());
    Assert.assertEquals(RuntimeScopes.wildcardAll(), RuntimeScopesExtractor.extract(
        jsonOverflowSpec, WORKFLOW_ID,
        Collections.singletonMap("big", Strings.repeat("a", 70_000)),
        TriggerMappedArguments.Result.empty(), NOOP_READER));

    RuntimeScopesExtractor.ConnectionReader bigConnReader = connName -> Optional.of(
        Collections.singletonMap("p", "${big}"));
    Assert.assertEquals(RuntimeScopes.wildcardAll(), RuntimeScopesExtractor.extract(
        specWithSingleProp("${conn(c1)}"), WORKFLOW_ID,
        Collections.singletonMap("big", Strings.repeat("a", 70_000)),
        TriggerMappedArguments.Result.empty(), bigConnReader));

    // 7. Large (> 64 KiB) literal property and configuration containing a normal ${secure(s1)}
    //    macro must NOT trip the growth budget and must extract exact narrow scopes.
    String largeLiteral = Strings.repeat("x", 80_000) + "${secure(s1)}"
        + Strings.repeat("y", 10_000);
    String largeConfig = "{\"query\":\"" + Strings.repeat("z", 80_000) + "${secure(s2)}\"}";
    Plugin plugin = makePlugin("batchsource", "Src", Collections.emptyMap(),
        Collections.singletonMap("prop", largeLiteral));
    ApplicationSpecification largeSpec = makeSpec(
        SYSTEM_ARTIFACT, largeConfig, Collections.singletonMap("src", plugin));
    RuntimeScopes largeLiteralScopes = RuntimeScopesExtractor.extract(
        largeSpec, WORKFLOW_ID, Collections.emptyMap(),
        TriggerMappedArguments.Result.empty(), NOOP_READER);
    Assert.assertEquals(
        new RuntimeScopes(Collections.emptySet(), Collections.emptySet(),
            ImmutableSet.of("s1", "s2"), false),
        largeLiteralScopes);
  }

  private static ApplicationSpecification specWithSingleProp(String value) {
    Plugin plugin = makePlugin("batchsource", "Src", Collections.emptyMap(),
        Collections.singletonMap("prop", value));
    return makeSpec(SYSTEM_ARTIFACT, null, Collections.singletonMap("src", plugin));
  }

  private static ApplicationSpecification makeSpec(ArtifactId artifactId,
      @Nullable String configuration, Map<String, Plugin> plugins) {
    return new DefaultApplicationSpecification(
        "pipe1", "1.0", null, "desc", configuration, artifactId,
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyMap(), plugins);
  }

  private static Plugin makePlugin(String type, String name,
      Map<String, PluginPropertyField> fields, Map<String, String> properties) {
    return makePlugin(SYSTEM_ARTIFACT, type, name, fields, properties);
  }

  private static Plugin makePlugin(@Nullable ArtifactId artifactId, String type, String name,
      Map<String, PluginPropertyField> fields, Map<String, String> properties) {
    PluginClass pluginClass = PluginClass.builder()
        .setType(type)
        .setName(name)
        .setDescription("desc")
        .setClassName("io.cdap.TestPlugin")
        .setProperties(fields)
        .build();
    return new Plugin(Collections.emptyList(), artifactId, pluginClass,
        PluginProperties.builder().addAll(properties).build());
  }
}
