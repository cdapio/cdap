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
import io.kubernetes.client.openapi.models.V1NamespaceList;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Collections;
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

    V1NamespaceList returnedNamespaceList = new V1NamespaceList();
    V1Namespace returnedNamespace = new V1Namespace().metadata(new V1ObjectMeta().name(KUBE_NAMESPACE));
    returnedNamespaceList.setItems(Collections.singletonList(returnedNamespace));
    when(coreV1Api.listNamespace(any(), any(), any(), eq(String.format("metadata.name=%s", KUBE_NAMESPACE)), any(),
            any(), any(), any(), any(), any()))
            .thenReturn(returnedNamespaceList);

    thrown.expect(IOException.class);
    thrown.expectMessage(String.format("Kubernetes namespace %s already exists", KUBE_NAMESPACE));
    kubeMasterEnvironment.onNamespaceCreation(CDAP_NAMESPACE, properties);
  }

  @Test
  public void testOnNamespaceCreationWithKubernetesError() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);

    when(coreV1Api.listNamespace(any(), any(), any(), eq(String.format("metadata.name=%s", KUBE_NAMESPACE)), any(),
            any(), any(), any(), any(), any()))
            .thenReturn(new V1NamespaceList());
    when(coreV1Api.createNamespace(any(), any(), any(), any()))
            .thenThrow(new ApiException());
    when(coreV1Api.deleteNamespace(eq(KUBE_NAMESPACE), any(), any(), any(), any(), any(), any()))
            .thenThrow(new ApiException(HttpURLConnection.HTTP_NOT_FOUND, "message"));

    thrown.expect(IOException.class);
    thrown.expectMessage("Error occurred while creating Kubernetes namespace.");
    kubeMasterEnvironment.onNamespaceCreation(CDAP_NAMESPACE, properties);
  }

  @Test
  public void testOnNamespaceCreationWithSuppressedDeletionError() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(KubeMasterEnvironment.NAMESPACE_PROPERTY, KUBE_NAMESPACE);

    when(coreV1Api.listNamespace(any(), any(), any(), eq(String.format("metadata.name=%s", KUBE_NAMESPACE)), any(),
            any(), any(), any(), any(), any()))
            .thenReturn(new V1NamespaceList());
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
    when(coreV1Api.deleteNamespace(eq(KUBE_NAMESPACE), any(), any(), any(), any(), any(), any()))
            .thenThrow(new ApiException(HttpURLConnection.HTTP_NOT_FOUND, "message"));
    try {
      kubeMasterEnvironment.onNamespaceDeletion(CDAP_NAMESPACE, properties);
    } catch (Exception e) {
      Assert.fail("Kubernetes deletion should not error if namespace does not exist. Exception: " + e);
    }
  }
}
