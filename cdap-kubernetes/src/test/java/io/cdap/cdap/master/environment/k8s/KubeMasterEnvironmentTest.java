/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.master.environment.k8s;

import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link KubeMasterEnvironment}
 */
public class KubeMasterEnvironmentTest {

  private static final String CDAP_NAMESPACE = "TEST_CDAP_Namespace";
  private static final String KUBE_NAMESPACE = "test-kube-namespace";

  private CoreV1Api coreV1Api;
  private KubeMasterEnvironment kubeMasterEnvironment;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void init() {
    coreV1Api = mock(CoreV1Api.class);
    kubeMasterEnvironment = new KubeMasterEnvironment();
    kubeMasterEnvironment.setCoreV1Api(coreV1Api);
    kubeMasterEnvironment.setNamespaceCreationEnabled();
  }

  @Test
  public void testOnNamespaceCreationWithNoNamespace() throws Exception {
    Map<String, String> properties = new HashMap<>();
    thrown.expect(IOException.class);
    thrown.expectMessage(String.format("Cannot create Kubernetes namespace for %s because no name was provided",
            CDAP_NAMESPACE));
    kubeMasterEnvironment.onNamespaceCreation(CDAP_NAMESPACE, properties);
  }

  @Test
  public void testOnNamespaceCreationWithExistingNamespace() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);

    V1Namespace returnedNamespace = new V1Namespace().metadata(new V1ObjectMeta().name(KUBE_NAMESPACE));
    when(coreV1Api.readNamespace(eq(KUBE_NAMESPACE), any(), any(), any())).thenReturn(returnedNamespace);

    thrown.expect(IOException.class);
    thrown.expectMessage(String.format("Kubernetes namespace %s exists but was not created by CDAP", KUBE_NAMESPACE));
    kubeMasterEnvironment.onNamespaceCreation(CDAP_NAMESPACE, properties);
  }

  @Test
  public void testOnNamespaceCreationWithExistingNamespaceAndWrongCdapInstance() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);

    V1ObjectMeta returnedMeta = new V1ObjectMeta().name(KUBE_NAMESPACE)
      .putLabelsItem("cdap.namespace", "wrong namespace");
    V1Namespace returnedNamespace = new V1Namespace().metadata(returnedMeta);
    when(coreV1Api.readNamespace(eq(KUBE_NAMESPACE), any(), any(), any())).thenReturn(returnedNamespace);

    thrown.expect(IOException.class);
    thrown.expectMessage(String.format("Kubernetes namespace %s exists but was not created by CDAP namespace %s",
                                       KUBE_NAMESPACE, CDAP_NAMESPACE));
    kubeMasterEnvironment.onNamespaceCreation(CDAP_NAMESPACE, properties);
  }

  @Test
  public void testOnNamespaceCreationSuccess() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);
    when(coreV1Api.readNamespace(eq(KUBE_NAMESPACE), any(), any(), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_NOT_FOUND, "namespace not found"));
    try {
      kubeMasterEnvironment.onNamespaceCreation(CDAP_NAMESPACE, properties);
    } catch (Exception e) {
      Assert.fail("Kubernetes creation should not error if namespace does not exist. Exception: " + e);
    }
  }

  @Test
  public void testOnNamespaceCreationWithSuppressedDeletionError() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);

    V1ObjectMeta returnedMeta = new V1ObjectMeta().name(KUBE_NAMESPACE)
      .putLabelsItem("cdap.namespace", CDAP_NAMESPACE);
    V1Namespace returnedNamespace = new V1Namespace().metadata(returnedMeta);

    // throw ApiException when coreV1Api.readNamespace() is called in findOrCreateKubeNamespace()
    // return returnedNamespace when coreV1Api.readNamespace() is called in deleteKubeNamespace()
    when(coreV1Api.readNamespace(eq(KUBE_NAMESPACE), any(), any(), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_NOT_FOUND, "namespace not found"))
      .thenReturn(returnedNamespace);
    when(coreV1Api.createNamespace(any(), any(), any(), any()))
      .thenThrow(new ApiException());
    when(coreV1Api.deleteNamespace(eq(KUBE_NAMESPACE), any(), any(), any(), any(), any(), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_INTERNAL_ERROR, "internal error message"));

    try {
      kubeMasterEnvironment.onNamespaceCreation(CDAP_NAMESPACE, properties);
    } catch (IOException e) {
      Assert.assertThat(e.getMessage(),
                        CoreMatchers.containsString("Error occurred while creating Kubernetes namespace"));
      Assert.assertEquals(1, e.getCause().getSuppressed().length);
      Assert.assertThat(e.getCause().getSuppressed()[0].getMessage(),
                        CoreMatchers.containsString("Error occurred while deleting Kubernetes namespace."));
    }
  }

  @Test
  public void testOnNamespaceDeletionWithKubernetesNotFoundError() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);
    when(coreV1Api.readNamespace(eq(KUBE_NAMESPACE), any(), any(), any()))
      .thenThrow(new ApiException(HttpURLConnection.HTTP_NOT_FOUND, "namespace not found"));
    try {
      kubeMasterEnvironment.onNamespaceDeletion(CDAP_NAMESPACE, properties);
    } catch (Exception e) {
      Assert.fail("Kubernetes deletion should not error if namespace does not exist. Exception: " + e);
    }
  }
}
