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

package io.cdap.cdap.security.encryption.guice;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.cdap.cdap.security.spi.encryption.AeadCipherCryptor;
import io.cdap.cdap.security.spi.encryption.AeadCipherContext;
import io.cdap.cdap.security.spi.encryption.CipherInitializationException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

/**
 * Tests for {@link LazyDelegateAeadCipher}.
 */
public class LazyDelegateAeadCipherTest {

  @Test
  public void testLazyInitializeOnEncryption() throws Exception {
    Map<String, String> properties = new HashMap<>();
    Map<String, String> secureProperties = new HashMap<>();
    AeadCipherCryptor mockAeadCipherCryptor = mock(AeadCipherCryptor.class);
    LazyDelegateAeadCipher delegate = new LazyDelegateAeadCipher(mockAeadCipherCryptor, properties,
        secureProperties);
    delegate.encrypt("some-data".getBytes(), "some-associated-data".getBytes());
    verify(mockAeadCipherCryptor)
        .initialize(argThat(new AeadCipherContextMatcher(properties, secureProperties)));
  }

  @Test
  public void testLazyInitializeOnDecryption() throws Exception {
    Map<String, String> properties = new HashMap<>();
    Map<String, String> secureProperties = new HashMap<>();
    AeadCipherCryptor mockAeadCipherCryptor = mock(AeadCipherCryptor.class);
    LazyDelegateAeadCipher delegate = new LazyDelegateAeadCipher(mockAeadCipherCryptor, properties,
        secureProperties);
    delegate.decrypt("some-data".getBytes(), "some-associated-data".getBytes());
    verify(mockAeadCipherCryptor)
        .initialize(argThat(new AeadCipherContextMatcher(properties, secureProperties)));
  }

  @Test(expected = CipherInitializationException.class)
  public void testLazyInitializeOnEncryptionException() throws Exception {
    Map<String, String> properties = new HashMap<>();
    Map<String, String> secureProperties = new HashMap<>();
    AeadCipherCryptor mockAeadCipherCryptor = mock(AeadCipherCryptor.class);
    doThrow(new CipherInitializationException("some initialization error")).when(
        mockAeadCipherCryptor)
        .initialize(any());
    LazyDelegateAeadCipher delegate = new LazyDelegateAeadCipher(mockAeadCipherCryptor, properties,
        secureProperties);
    delegate.encrypt("some-data".getBytes(), "some-associated-data".getBytes());
  }

  @Test(expected = CipherInitializationException.class)
  public void testLazyInitializeOnDecryptionException() throws Exception {
    Map<String, String> properties = new HashMap<>();
    Map<String, String> secureProperties = new HashMap<>();
    AeadCipherCryptor mockAeadCipherCryptor = mock(AeadCipherCryptor.class);
    doThrow(new CipherInitializationException("some initialization error")).when(
        mockAeadCipherCryptor)
        .initialize(any());
    LazyDelegateAeadCipher delegate = new LazyDelegateAeadCipher(mockAeadCipherCryptor, properties,
        secureProperties);
    delegate.decrypt("some-data".getBytes(), "some-associated-data".getBytes());
  }

  private class AeadCipherContextMatcher extends ArgumentMatcher<AeadCipherContext> {

    Map<String, String> properties;
    Map<String, String> secureProperties;

    AeadCipherContextMatcher(Map<String, String> properties, Map<String, String> secureProperties) {
      this.properties = properties;
      this.secureProperties = secureProperties;
    }

    @Override
    public boolean matches(Object o) {
      if (!(o instanceof AeadCipherContext)) {
        return false;
      }
      AeadCipherContext aeadCipherContext = (AeadCipherContext) o;
      return properties.equals(aeadCipherContext.getProperties()) && secureProperties
          .equals(aeadCipherContext.getSecureProperties());
    }
  }
}
