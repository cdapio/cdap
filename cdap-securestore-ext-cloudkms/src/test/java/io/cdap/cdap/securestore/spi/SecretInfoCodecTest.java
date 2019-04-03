/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.securestore.spi;

import io.cdap.cdap.securestore.gcp.cloudkms.SecretInfo;
import io.cdap.cdap.securestore.gcp.cloudkms.SecretInfoCodec;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for encoding and decoding {@link SecretInfo}.
 */
public class SecretInfoCodecTest {

  @Test
  public void encodeDecodeSecretInfo() throws Exception {
    SecretInfoCodec encoderDecoder = new SecretInfoCodec();
    String secretData = "secretData";
    long creationTime = System.currentTimeMillis();
    Map<String, String> properties = new HashMap<>();
    SecretInfo secretInfo = new SecretInfo("secretName", "description", secretData.getBytes(StandardCharsets.UTF_8),
                                           creationTime, properties);
    byte[] encodedSecretInfo = encoderDecoder.encode(secretInfo);
    SecretInfo decodedSecretInfo = encoderDecoder.decode(encodedSecretInfo);

    Assert.assertArrayEquals(secretInfo.getSecretData(), decodedSecretInfo.getSecretData());
    Assert.assertEquals(secretInfo.getName(), decodedSecretInfo.getName());
    Assert.assertEquals(secretInfo.getDescription(), decodedSecretInfo.getDescription());
    Assert.assertEquals(secretInfo.getCreationTimeMs(), decodedSecretInfo.getCreationTimeMs());
    Assert.assertEquals(secretInfo.getDescription(), decodedSecretInfo.getDescription());
    Assert.assertEquals(secretInfo.getProperties().size(), decodedSecretInfo.getProperties().size());
  }

  @Test
  public void encodeDecodeSecretInfoWithNull() throws Exception {
    SecretInfoCodec encoderDecoder = new SecretInfoCodec();
    String secretData = "secretData";
    long creationTime = System.currentTimeMillis();
    Map<String, String> properties = new HashMap<>();
    SecretInfo secretInfo = new SecretInfo("secretName", null, secretData.getBytes(StandardCharsets.UTF_8),
                                           creationTime, properties);
    byte[] encodedSecretInfo = encoderDecoder.encode(secretInfo);
    SecretInfo decodedSecretInfo = encoderDecoder.decode(encodedSecretInfo);

    Assert.assertArrayEquals(secretInfo.getSecretData(), decodedSecretInfo.getSecretData());
    Assert.assertEquals(secretInfo.getName(), decodedSecretInfo.getName());
    Assert.assertEquals(secretInfo.getDescription(), decodedSecretInfo.getDescription());
    Assert.assertEquals(secretInfo.getCreationTimeMs(), decodedSecretInfo.getCreationTimeMs());
    Assert.assertEquals(secretInfo.getDescription(), decodedSecretInfo.getDescription());
    Assert.assertEquals(secretInfo.getProperties().size(), decodedSecretInfo.getProperties().size());
  }
}
