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

package io.cdap.cdap.securestore.gcp.cloudsecretmanager;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.securestore.spi.SecretNotFoundException;
import io.cdap.cdap.securestore.spi.secret.Secret;
import io.cdap.cdap.securestore.spi.secret.SecretMetadata;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doThrow;

public class GcpSecretManagerTest {
  @Rule
  public final MockitoRule mockito = MockitoJUnit.rule();

  private static final String NAMESPACE = "test_namespace";

  @Mock
  private CloudSecretManagerClient client;

  private GcpSecretManager secretManager;

  @Before
  public void setUp() {
    secretManager = new GcpSecretManager();
    secretManager.initialize(client);
  }

  @Test
  public void store_create() throws Exception {
    ApiException exception = createApiException(Code.NOT_FOUND);
    when(client.getSecret(ArgumentMatchers.any(), ArgumentMatchers.any())).thenThrow(exception);

    secretManager.store(NAMESPACE, createSecret("example"));

    verify(client, times(1)).createSecret(ArgumentMatchers.any());
    verify(client, times(1)).addSecretVersion(ArgumentMatchers.any(), ArgumentMatchers.any());
    verify(client, times(0)).updateSecret(ArgumentMatchers.any());
  }

  @Test
  public void store_updateMetadataOnly() throws Exception {
    byte[] payload = "Foo".getBytes();
    SecretMetadata metadata = createMetadata("example");
    when(client.getSecret(ArgumentMatchers.any(), ArgumentMatchers.any()))
      .thenReturn(WrappedSecret.fromMetadata(NAMESPACE, metadata));
    when(client.getSecretData(ArgumentMatchers.any())).thenReturn(payload);

    // Metadata [potentially] updated, payload remains the same.
    secretManager.store(NAMESPACE, new Secret(payload, metadata));

    // Update, not create called. No secret version added.
    verify(client, times(0)).createSecret(ArgumentMatchers.any());
    verify(client, times(1)).updateSecret(ArgumentMatchers.any());
    verify(client, times(0)).addSecretVersion(ArgumentMatchers.any(), ArgumentMatchers.any());
  }

  @Test
  public void store_updatePayload() throws Exception {
    byte[] oldPayload = "Foo".getBytes();
    byte[] newPayload = "Bar".getBytes();
    SecretMetadata metadata = createMetadata("example");
    when(client.getSecret(ArgumentMatchers.any(), ArgumentMatchers.any()))
      .thenReturn(WrappedSecret.fromMetadata(NAMESPACE, metadata));
    when(client.getSecretData(ArgumentMatchers.any())).thenReturn(oldPayload);

    secretManager.store(NAMESPACE, new Secret(newPayload, metadata));

    // Update called, secret version added.
    verify(client, times(1)).updateSecret(ArgumentMatchers.any());
    verify(client, times(1)).addSecretVersion(ArgumentMatchers.any(), eq(newPayload));
  }

  @Test
  public void store_exception() throws Exception {
    ApiException exception = createApiException(Code.PERMISSION_DENIED);
    when(client.getSecret(ArgumentMatchers.any(), ArgumentMatchers.any()))
      .thenThrow(exception);

    assertThrows(IOException.class, () -> secretManager.store(NAMESPACE, createSecret("example")));
  }

  @Test
  public void get_success() throws Exception {
    byte[] payload = "example".getBytes();
    SecretMetadata metadata = createMetadata("example");
    when(client.getSecret(ArgumentMatchers.any(), ArgumentMatchers.any()))
      .thenReturn(WrappedSecret.fromMetadata(NAMESPACE, metadata));
    when(client.getSecretData(ArgumentMatchers.any())).thenReturn(payload);

    Secret secret = secretManager.get(NAMESPACE, "example");

    assertArrayEquals(payload, secret.getData());
  }

  @Test
  public void get_notFoundError() throws Exception {
    ApiException exception = createApiException(Code.NOT_FOUND);
    when(client.getSecret(ArgumentMatchers.any(), ArgumentMatchers.any())).thenThrow(exception);

    assertThrows(SecretNotFoundException.class, () -> secretManager.get(NAMESPACE, "example"));
  }

  @Test
  public void get_otherError() throws Exception {
    ApiException exception = createApiException(Code.PERMISSION_DENIED);
    when(client.getSecret(ArgumentMatchers.any(), ArgumentMatchers.any())).thenThrow(exception);

    assertThrows(IOException.class, () -> secretManager.get(NAMESPACE, "example"));
  }

  @Test
  public void list_success() throws Exception {
    SecretMetadata item1 = createMetadata("example1");
    SecretMetadata item2 = createMetadata("example2");
    when(client.listSecrets(ArgumentMatchers.any())).thenReturn(
      ImmutableList.of(
        WrappedSecret.fromMetadata(NAMESPACE, item1),
        WrappedSecret.fromMetadata(NAMESPACE, item2)));

    ImmutableList<SecretMetadata> list = ImmutableList.copyOf(secretManager.list(NAMESPACE));

    assertEquals(ImmutableList.of(item1, item2), list);
  }

  @Test
  public void list_wrapsApiExceptions() throws Exception {
    ApiException exception = createApiException(Code.PERMISSION_DENIED);
    when(client.listSecrets(ArgumentMatchers.any())).thenThrow(exception);

    assertThrows(IOException.class, () -> secretManager.list(NAMESPACE));
  }

  @Test
  public void delete_wrapsApiExceptions() throws Exception {
    ApiException exception = createApiException(Code.PERMISSION_DENIED);
    doThrow(exception).when(client).deleteSecret(ArgumentMatchers.any(), ArgumentMatchers.any());

    assertThrows(IOException.class, () -> secretManager.delete(NAMESPACE, "example"));
  }

  private static Secret createSecret(String name) {
    return new Secret(name.getBytes(), createMetadata(name));
  }

  private static SecretMetadata createMetadata(String name) {
    return new SecretMetadata(name, "Fake description", 0, ImmutableMap.of());
  }

  private static ApiException createApiException(Code code) {
    return new ApiException(new RuntimeException("Fake Exception"), createStatusCode(code), true);
  }

  private static StatusCode createStatusCode(Code code) {
    return new StatusCode() {
      @Override
      public Code getCode() {
        return code;
      }

      @Override
      public Object getTransportCode() {
        throw new RuntimeException("unimplemented");
      }
    };
  }
}
