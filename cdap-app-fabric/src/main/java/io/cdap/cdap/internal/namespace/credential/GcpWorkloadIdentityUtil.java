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

package io.cdap.cdap.internal.namespace.credential;

import com.google.common.hash.Hashing;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.credential.NamespaceWorkloadIdentity;
import io.cdap.cdap.proto.id.NamespaceId;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Utility class for {@link NamespaceWorkloadIdentity} associated with the namespace.
 */
public final class GcpWorkloadIdentityUtil {

  private static final String NAMESPACE_IDENTITY_NAME_PREFIX = "ns-gcp-wi";

  public static final String SYSTEM_PROFILE_NAME = "ns-gcp-wi";

  private static final String GCP_OAUTH_SCOPES_PROPERTY = "gcp.oauth.scopes";
  private static final String K8S_NAMESPACE_PROPERTY = "k8s.namespace";
  private static final String WRAPPED_CDAP_NAMESPACE_PROPERTY = "gcp.wrapped.cdap.namespace";

  /**
   * Returns the namespace workload identity name.
   *
   * @param namespaceId The namespace which the identity is attached to.
   * @return namespace workload identity name.
   */
  public static String getWorkloadIdentityName(NamespaceId namespaceId) {
    return String.format("%s-%s", NAMESPACE_IDENTITY_NAME_PREFIX,
        computeLengthLimitedIdentity(namespaceId));
  }

  /**
   * Computes unique namespace identity name by lowercase namespace id truncated to 15 characters
   * appended with the first 15 hex characters of the namespace's SHA256.
   *
   * @param namespaceId the {@link NamespaceId} of the namespace.
   * @return namespace unique identity name.
   */
  private static String computeLengthLimitedIdentity(NamespaceId namespaceId) {
    if (NamespaceId.DEFAULT.equals(namespaceId)) {
      return namespaceId.getNamespace();
    }
    String namespace = namespaceId.getNamespace();
    String sha256Hex = Hashing.sha256().hashString(namespace, StandardCharsets.UTF_8).toString();
    sha256Hex = sha256Hex.length() > 15 ? sha256Hex.substring(0, 15) : sha256Hex;
    namespace = namespace.length() > 15 ? namespace.substring(0, 15) : namespace;
    return String.format("%s-%s", namespace, sha256Hex).toLowerCase().replace('_', '-');
  }

  /**
   * Constructs the provisioning properties to pass to the credential provider.
   *
   * @param scopes The GCP OAuth scopes to request.
   * @param namespaceMeta The metadata for the namespace.
   * @return A property map for credential provisioning.
   */
  public static Map<String, String> createProvisionPropertiesMap(@Nullable String scopes,
      NamespaceMeta namespaceMeta) {
    Map<String, String> properties = new HashMap<>();
    if (scopes != null) {
      properties.put(GCP_OAUTH_SCOPES_PROPERTY, scopes);
    }
    // Add property for k8s namespace tied to a CDAP namespace. This is used in Hybrid mode.
    properties
        .put(K8S_NAMESPACE_PROPERTY, namespaceMeta.getConfig().getConfig(K8S_NAMESPACE_PROPERTY));
    // Add namespace property indicating which CDAP namespace the parent resource was operated on.
    // This is necessary because the namespace field will be the SYSTEM namespace in which all
    // managed workload identity resources are stored.
    properties.put(WRAPPED_CDAP_NAMESPACE_PROPERTY, namespaceMeta.getNamespaceId().getNamespace());
    return properties;
  }
}
