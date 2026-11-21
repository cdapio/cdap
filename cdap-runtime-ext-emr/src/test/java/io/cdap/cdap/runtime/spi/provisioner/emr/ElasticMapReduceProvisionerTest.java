/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.provisioner.emr;

import com.amazonaws.Request;
import com.amazonaws.services.ec2.model.ImportKeyPairRequest;
import com.amazonaws.services.ec2.model.transform.ImportKeyPairRequestMarshaller;
import com.amazonaws.util.Base64;
import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for Amazon EMR provisioner
 */
public class ElasticMapReduceProvisionerTest {

  @XmlRootElement(name = "keyPairPayload")
  public static class SampleXmlPayload {
    @XmlElement
    public String keyName;
  }

  @Test
  public void testJaxbRuntimeAvailableOnJava11() throws Exception {
    JAXBContext jaxbContext = JAXBContext.newInstance(SampleXmlPayload.class);
    SampleXmlPayload payload = new SampleXmlPayload();
    payload.keyName = "cdap-emr-key";
    StringWriter writer = new StringWriter();
    jaxbContext.createMarshaller().marshal(payload, writer);
    SampleXmlPayload roundTrip = (SampleXmlPayload) jaxbContext.createUnmarshaller()
        .unmarshal(new StringReader(writer.toString()));
    Assert.assertEquals("cdap-emr-key", roundTrip.keyName);

    // AWS SDK Base64 and EC2 request marshalling need JAXB on Java 11.
    Assert.assertEquals("AQID", Base64.encodeAsString(new byte[]{1, 2, 3}));
    Request<ImportKeyPairRequest> request = new ImportKeyPairRequestMarshaller()
        .marshall(new ImportKeyPairRequest("cdap-key", "ssh-rsa AAAAB3Nza"));
    Assert.assertEquals("cdap-key", request.getParameters().get("KeyName").get(0));
  }

  @Test
  public void testValidatePropertiesUsesExtensionClassLoader() throws Exception {
    ElasticMapReduceProvisioner provisioner = new ElasticMapReduceProvisioner();
    ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
    try (URLClassLoader emptyClassLoader = new URLClassLoader(new URL[0], null)) {
      Thread.currentThread().setContextClassLoader(emptyClassLoader);

      AtomicReference<ClassLoader> observedClassLoader = new AtomicReference<>();
      Map<String, String> properties = new HashMap<String, String>() {
        @Override
        public String get(Object key) {
          observedClassLoader.set(Thread.currentThread().getContextClassLoader());
          return super.get(key);
        }
      };
      properties.put("accessKey", "ak");
      properties.put("secretKey", "sk");
      properties.put("ec2SubnetId", "subnet-1");
      properties.put("additionalMasterSecurityGroup", "sg-1");

      provisioner.validateProperties(properties);
      Assert.assertSame(ElasticMapReduceProvisioner.class.getClassLoader(),
          observedClassLoader.get());
      Assert.assertSame(emptyClassLoader, Thread.currentThread().getContextClassLoader());

      try {
        provisioner.validateProperties(Collections.emptyMap());
        Assert.fail("Expected IllegalArgumentException for missing properties");
      } catch (IllegalArgumentException expected) {
        Assert.assertSame(emptyClassLoader, Thread.currentThread().getContextClassLoader());
      }
    } finally {
      Thread.currentThread().setContextClassLoader(oldClassLoader);
    }
  }

  @Test
  public void testClusterName() {
    // test basic
    ProgramRunInfo programRunInfo = new ProgramRunInfo.Builder()
      .setNamespace("ns")
      .setApplication("app")
      .setVersion("1.0")
      .setProgramType("workflow")
      .setProgram("program")
      .setRun(UUID.randomUUID().toString())
      .build();
    Assert.assertEquals("cdap-app-" + programRunInfo.getRun(),
                        ElasticMapReduceProvisioner.getClusterName(programRunInfo));

    // test lowercasing, stripping of invalid characters, and truncation
    programRunInfo = new ProgramRunInfo.Builder()
      .setNamespace("ns")
      .setApplication("My@Appl!cation")
      .setVersion("1.0")
      .setProgramType("workflow")
      .setProgram("program")
      .setRun(UUID.randomUUID().toString())
      .build();
    Assert.assertEquals("cdap-myapplcat-" + programRunInfo.getRun(),
                        ElasticMapReduceProvisioner.getClusterName(programRunInfo));
  }
}
