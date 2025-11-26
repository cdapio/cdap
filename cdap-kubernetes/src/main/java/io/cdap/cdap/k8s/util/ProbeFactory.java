/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.k8s.util;

import com.google.common.base.Preconditions;
import io.kubernetes.client.openapi.models.V1HTTPGetAction;
import io.kubernetes.client.openapi.models.V1Probe;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Utility class to create a {@link V1Probe} from the given map of configs.
 *
 * Usage for k8s probe :
 * Please refer to this class's parameter constants for forming full
 * config name regarding a k8s probe.
 * Example : for PROBE_INIT_DELAY_SEC = "task.worker.probe.k8s.liveness.init.delay.seconds"`
 * The format is :
 * TASK_WORKER_PROBE_PREFIX + ENV + PROBE_NAME + . + PARAMETER_CONSTANT
 * task.worker.probe        + k8s + liveness   + . + init.delay.seconds
 *   //
 * TASK_WORKER_PROBE_PREFIX = it is defined in constants (Ex : `task.worker.probe.`)
 * ENV = The value from Constant defined in an Environment Class. Example : `ProbeFactory`'s PROBE_ENV = k8s
 *   //
 * PROBE_NAME =
 *    it's the cConf valueOf [ TASK_WORKER_PROBE_PREFIX
 *    + ENV + PROBE_NAME_CSV (constant in ProbeFactory) ],
 *    (Ex : `task.worker.probe.k8s.names` = `liveness,readiness`)
 * PARAMETER_CONSTANT = it's the value of a CONSTANT in `ProbeFactory` (Ex : `init.delay.seconds`)
 *   //
 * Minimum required fields for k8s :
 * `task.worker.probe.enabled` = true
 * `task.worker.probe.k8s.names`
 * `task.worker.probe.k8s.<PROBE_NAME>.type`
 *
 */
public final class ProbeFactory {

  public enum ProbeName {
    LIVENESS,
    READINESS,
    STARTUP
  }

  public static final String PROBE_ENV = "k8s.";
  public static final String PROBE_NAME_CSV = "names";

  // --- Common Parameter Configs ---
  private static final String PROBE_ACTION_TYPE = "type";
  private static final String PROBE_INIT_DELAY_SEC = "init.delay.seconds";
  private static final String PROBE_TIMEOUT_SEC = "timeout.seconds";
  private static final String PROBE_FAILURE_THRESHOLD = "failure.threshold";
  private static final String PROBE_SUCCESS_THRESHOLD = "success.threshold";
  private static final String PROBE_PERIOD_SECONDS = "period.seconds";
  private static final String PROBE_TERM_GRACE_PERIOD_SECONDS =
      "termination.grace.period.seconds";

  // HTTP Probe Specific Keys
  private static final String PROBE_HTTP_PATH = "http.path";
  private static final String PROBE_HTTP_PORT = "http.port";
  private static final String PROBE_HTTP_HOST = "http.host";
  private static final String PROBE_HTTP_SCHEME = "http.scheme";

  private ProbeFactory() {}

  public static V1Probe createV1Probe(Map<String, String> probeConf) {
    V1Probe probe = new V1Probe();
    setCommonProbeFields(probe, probeConf);

    Preconditions.checkArgument(probeConf.get(PROBE_ACTION_TYPE) != null, "Probe Type cannot be null.");
    // Only supporting HTTP for now.
    switch (probeConf.get(PROBE_ACTION_TYPE).trim().toLowerCase()) {
      case "httpget":
        probe.setHttpGet(createHTTPGetAction(probeConf));
        break;
      default:
        throw new IllegalStateException("Unsupported Probe Type: " + probeConf.get(PROBE_ACTION_TYPE));
    }
    return probe;
  }

  private static void setCommonProbeFields(V1Probe probe, Map<String, String> probeConf) {
    // 1. initialDelaySeconds (Integer, min 0)
    setProbeFieldIfPresent(
        probeConf,
        PROBE_INIT_DELAY_SEC,
        Integer::valueOf,
        (val) -> val >= 0,
        "initialDelaySeconds cannot be less than 0",
        probe::initialDelaySeconds);

    // 2. timeoutSeconds (Integer, min 1)
    setProbeFieldIfPresent(
        probeConf,
        PROBE_TIMEOUT_SEC,
        Integer::valueOf,
        (val) -> val >= 1,
        "timeoutSeconds cannot be less than 1",
        probe::timeoutSeconds);

    // 3. failureThreshold (Integer, min 1)
    setProbeFieldIfPresent(
        probeConf,
        PROBE_FAILURE_THRESHOLD,
        Integer::valueOf,
        (val) -> val >= 1,
        "failureThreshold cannot be less than 1",
        probe::failureThreshold);

    // 4. successThreshold (Integer, min 1)
    setProbeFieldIfPresent(
        probeConf,
        PROBE_SUCCESS_THRESHOLD,
        Integer::valueOf,
        (val) -> val >= 1,
        "successThreshold cannot be less than 1",
        probe::successThreshold);

    // 5. periodSeconds (Integer, min 1)
    setProbeFieldIfPresent(
        probeConf,
        PROBE_PERIOD_SECONDS,
        Integer::valueOf,
        (val) -> val >= 1,
        "periodSeconds cannot be less than 1",
        probe::periodSeconds);

    // 6. terminationGracePeriodSeconds (Long, min 1)
    setProbeFieldIfPresent(
        probeConf,
        PROBE_TERM_GRACE_PERIOD_SECONDS,
        Long::valueOf,
        (val) -> val >= 1,
        "terminationGracePeriodSeconds cannot be less than 1",
        probe::terminationGracePeriodSeconds);
  }

  /**
   * Encapsulates the logic for extracting, validating, and setting a field on the V1Probe object.
   * @param <T> The target type of the field (e.g., Integer, Long).
   * @param probeConf The map containing probe configurations.
   * @param key The configuration key for the field.
   * @param converter A function to convert the String value to type T.
   * @param validator A predicate to validate the converted value (e.g., check for min value).
   * @param errorMessage The message for the Preconditions.checkArgument if validation fails.
   * @param setter A consumer function (method reference) to set the value on the V1Probe object.
   */
  private static <T> void setProbeFieldIfPresent(
      Map<String, String> probeConf,
      String key,
      Function<String, T> converter,
      Function<T, Boolean> validator, // Changed to Function<T, Boolean> for clarity/simplicity in this pattern
      String errorMessage,
      Consumer<T> setter) {

    Optional<T> valueOpt = getValue(probeConf, key, converter);

    valueOpt.ifPresent(val -> {
      Preconditions.checkArgument(validator.apply(val), errorMessage);
      setter.accept(val);
    });
  }

  private static <T> Optional<T> getValue(Map<String, String> conf, String key, Function<String, T> converter) {
    String strVal = conf.get(key);
    T val = null;
    if (strVal != null) {
      val = converter.apply(strVal);
    }
    return Optional.ofNullable(val);
  }

  private static V1HTTPGetAction createHTTPGetAction(Map<String, String> probeConf) {
    V1HTTPGetAction action = new V1HTTPGetAction();

    // Port (required for an HTTP probe)
    Preconditions.checkArgument(probeConf.get(PROBE_HTTP_PORT) != null,
        "Port of a HTTP Probe cannot be null.");
    Optional.ofNullable(probeConf.get(PROBE_HTTP_PORT))
        .map(Integer::parseInt)
        .map(io.kubernetes.client.custom.IntOrString::new)
        .ifPresent(action::port);

    // Path (optional)
    Optional.ofNullable(probeConf.get(PROBE_HTTP_PATH))
        .ifPresent(action::path);

    // Host (optional) , If not set, it defaults to the pod IP, which is usually correct.
    Optional.ofNullable(probeConf.get(PROBE_HTTP_HOST))
        .ifPresent(action::host);

    // Scheme (optional)
    Optional.ofNullable(probeConf.get(PROBE_HTTP_SCHEME))
        .ifPresent(action::scheme);

    return action;
  }
}
