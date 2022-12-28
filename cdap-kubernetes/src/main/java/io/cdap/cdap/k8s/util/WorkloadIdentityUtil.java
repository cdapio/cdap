/*
 * Copyright © 2022 Cask Data, Inc.
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

import com.google.gson.Gson;
import io.cdap.cdap.k8s.identity.GCPWorkloadIdentityCredential;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapProjection;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1KeyToPath;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1ProjectedVolumeSource;
import io.kubernetes.client.openapi.models.V1ServiceAccountTokenProjection;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.kubernetes.client.openapi.models.V1VolumeProjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Utility methods for workload identity.
 */
public class WorkloadIdentityUtil {
  private static final Logger LOG = LoggerFactory.getLogger(WorkloadIdentityUtil.class);

  /**
   * Needs to be public to allow for filtering these environment variables from parent pods.
   */
  public static final String WORKLOAD_IDENTITY_ENV_VAR_KEY = "GOOGLE_APPLICATION_CREDENTIALS";
  public static final String WORKLOAD_IDENTITY_PROJECTED_VOLUME_NAME = "gcp-ksa";

  private static final String WORKLOAD_IDENTITY_CONFIGMAP_NAME = "workload-identity-config";
  private static final String WORKLOAD_IDENTITY_CREDENTIAL_DIR = "/var/run/secrets/tokens/gcp-ksa";
  private static final String WORKLOAD_IDENTITY_CONFIGMAP_KEY = "config";
  private static final String WORKLOAD_IDENTITY_CONFIGMAP_FILE = "google-application-credentials.json";
  private static final String WORKLOAD_IDENTITY_DATA_KEY = "config";
  private static final String WORKLOAD_IDENTITY_AUDIENCE_FORMAT = "identitynamespace:%s:%s";
  private static final String WORKLOAD_IDENTITY_IMPERSONATION_URL_FORMAT = "https://iamcredentials.googleapis.com/" +
    "v1/projects/-/serviceAccounts/%s:generateAccessToken";
  private static final String WORKLOAD_IDENTITY_TOKEN_URL = "https://sts.googleapis.com/v1/token";
  private static final String WORKLOAD_IDENTITY_CREDENTIAL_KSA_PATH = "token";
  private static final String WORKLOAD_IDENTITY_CREDENTIAL_KSA_SOURCE_PATH
    = WorkloadIdentityUtil.WORKLOAD_IDENTITY_CREDENTIAL_DIR + "/" + WORKLOAD_IDENTITY_CREDENTIAL_KSA_PATH;
  private static final String WORKLOAD_IDENTITY_CREDENTIAL_GSA_SOURCE_PATH
    = WorkloadIdentityUtil.WORKLOAD_IDENTITY_CREDENTIAL_DIR + "/" + WORKLOAD_IDENTITY_CONFIGMAP_FILE;
  private static final long WORKLOAD_IDENTITY_SERVICE_ACCOUNT_TOKEN_TTL_SECONDS_DEFAULT = 172800L;

  /**
   * Helper method for testing whether to mount a workload identity config map.
   * Workload identity should be mounted in the following two scenarios:
   *  - A program is running in the CDAP installation namespace
   *  - A program has a workload identity email set
   * This helper method helps keep the mounting logic consistent between KubeTwillPreparer and KubeMasterEnvironment.
   *
   * @param installK8sNamespace The k8s namespace CDAP is installed in
   * @param programRuntimeNamespace The namespace the program is running in
   * @param workloadIdentityServiceAccountEmail The email value for the namespace
   */
  public static boolean shouldMountWorkloadIdentity(String installK8sNamespace, String programRuntimeNamespace,
                                                 String workloadIdentityServiceAccountEmail) {
    return installK8sNamespace.equals(programRuntimeNamespace)
      || (workloadIdentityServiceAccountEmail != null && !workloadIdentityServiceAccountEmail.isEmpty());
  }

  /**
   * Finds or creates the ConfigMap which stores the GCP credentials for Fleet Workload Identity.
   * For details, see steps 6-7 of
   * https://cloud.google.com/anthos/multicluster-management/fleets/workload-identity#impersonate_a_service_account
   */
  public static void findOrCreateWorkloadIdentityConfigMap(CoreV1Api coreV1Api, String k8sNamespace,
                                                           String workloadIdentityGCPServiceAccountEmail,
                                                           String workloadIdentityPool,
                                                           String workloadIdentityProvider)
    throws ApiException, IOException {
    // Check if workload identity config map already exists
    try {
      coreV1Api.readNamespacedConfigMap(k8sNamespace, WORKLOAD_IDENTITY_CONFIGMAP_NAME, null);
      // Workload identity config map already exists, so return early
      LOG.debug("Workload identity config found, returning without creating it...");
      return;
    } catch (ApiException e) {
      if (e.getCode() == HttpURLConnection.HTTP_NOT_FOUND) {
        LOG.debug("Creating workload identity config map for kubernetes namespace {}", k8sNamespace);
      } else {
        throw new IOException("Failed to fetch existing workload identity config map. Error code = " + e.getCode() +
                                ", Body = " + e.getResponseBody(), e);
      }
    }

    String workloadIdentityAudience = String.format(WORKLOAD_IDENTITY_AUDIENCE_FORMAT, workloadIdentityPool,
                                                    workloadIdentityProvider);
    String workloadIdentityImpersonationURL = String.format(WORKLOAD_IDENTITY_IMPERSONATION_URL_FORMAT,
                                                            workloadIdentityGCPServiceAccountEmail);
    GCPWorkloadIdentityCredential credential =
      new GCPWorkloadIdentityCredential(GCPWorkloadIdentityCredential.CredentialType.EXTERNAL_ACCOUNT,
                                        workloadIdentityAudience, workloadIdentityImpersonationURL,
                                        GCPWorkloadIdentityCredential.TokenType.JWT, WORKLOAD_IDENTITY_TOKEN_URL,
                                        WORKLOAD_IDENTITY_CREDENTIAL_KSA_SOURCE_PATH);
    String workloadIdentityCredentialJSON = new Gson().toJson(credential);
    Map<String, String> workloadIdentityConfigMapData = new HashMap<>();
    workloadIdentityConfigMapData.put(WORKLOAD_IDENTITY_DATA_KEY, workloadIdentityCredentialJSON);
    V1ConfigMap configMap = new V1ConfigMap()
      .metadata(new V1ObjectMeta().namespace(k8sNamespace).name(WorkloadIdentityUtil.WORKLOAD_IDENTITY_CONFIGMAP_NAME))
      .data(workloadIdentityConfigMapData);
    coreV1Api.createNamespacedConfigMap(k8sNamespace, configMap, null, null, null, null);
  }

  /**
   * Creates a workload identity volume. For details, see steps 6-7 of
   * https://cloud.google.com/anthos/multicluster-management/fleets/workload-identity#impersonate_a_service_account
   *
   * @param workloadIdentityServiceAccountTokenTTLSeconds TTL for the KSA token.
   * @param workloadIdentityPool The workload identity pool to use.
   * @return a projected V1Volume containing workload identity credentials.
   */
  public static V1Volume generateWorkloadIdentityVolume(long workloadIdentityServiceAccountTokenTTLSeconds,
                                                        String workloadIdentityPool) {
    V1ServiceAccountTokenProjection serviceAccountTokenProjection = new V1ServiceAccountTokenProjection()
      .path(WORKLOAD_IDENTITY_CREDENTIAL_KSA_PATH)
      .expirationSeconds(workloadIdentityServiceAccountTokenTTLSeconds)
      .audience(workloadIdentityPool);
    V1ConfigMapProjection configMapProjection = new V1ConfigMapProjection()
      .name(WORKLOAD_IDENTITY_CONFIGMAP_NAME)
      .optional(false)
      .addItemsItem(new V1KeyToPath().key(WORKLOAD_IDENTITY_CONFIGMAP_KEY)
                      .path(WORKLOAD_IDENTITY_CONFIGMAP_FILE));
    return new V1Volume().name(WORKLOAD_IDENTITY_PROJECTED_VOLUME_NAME)
      .projected(new V1ProjectedVolumeSource().defaultMode(420)
                   .addSourcesItem(new V1VolumeProjection().serviceAccountToken(serviceAccountTokenProjection))
                   .addSourcesItem(new V1VolumeProjection().configMap(configMapProjection)));
  }

  /**
   * @return a V1VolumeMount for mounting workload identity credentials.
   */
  public static V1VolumeMount generateWorkloadIdentityVolumeMount() {
    return new V1VolumeMount()
      .name(WorkloadIdentityUtil.WORKLOAD_IDENTITY_PROJECTED_VOLUME_NAME)
      .mountPath(WORKLOAD_IDENTITY_CREDENTIAL_DIR).readOnly(true);
  }

  /**
   * @return the V1EnvVar for setting workload identity credentials.
   */
  public static V1EnvVar generateWorkloadIdentityEnvVar() {
    return new V1EnvVar().name(WORKLOAD_IDENTITY_ENV_VAR_KEY).value(WORKLOAD_IDENTITY_CREDENTIAL_GSA_SOURCE_PATH);
  }

  /**
   * Converts a string form of a the workload identity TTL to a long.
   * @param ttlStr The workload identity KSA TTL string. If null, defaults to 172800L.
   * @return The long form of the TTL.
   * @throws IllegalArgumentException if the TTL is less than or equal to 0.
   */
  public static long convertWorkloadIdentityTTLFromString(@Nullable String ttlStr) throws IllegalArgumentException {
    long ttl = WORKLOAD_IDENTITY_SERVICE_ACCOUNT_TOKEN_TTL_SECONDS_DEFAULT;
    if (ttlStr != null) {
      ttl = Long.parseLong(ttlStr);
    }
    if (ttl <= 0) {
      throw new IllegalArgumentException(String.format("Workload identity k8s service account token TTL '%d' " +
                                                         "cannot be less than zero", ttl));
    }
    return ttl;
  }
}
