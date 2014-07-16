/*
 * Copyright 2014 Continuuity, Inc.
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

package com.continuuity.jetstream.internal;

import com.continuuity.jetstream.flowlet.ConfigFileGenerator;
import com.continuuity.jetstream.flowlet.ConfigFileLocalizer;
import com.continuuity.jetstream.flowlet.InputFlowletSpecification;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * Creates Config Files for a given {@link com.continuuity.jetstream.flowlet.InputFlowletSpecification}
 * in a given Directory location.
 */
public class LocalConfigFileLocalizer implements ConfigFileLocalizer {

  private static final Logger LOG = LoggerFactory.getLogger(LocalConfigFileLocalizer.class);
  private File dir;
  private ConfigFileGenerator generator;

  public LocalConfigFileLocalizer(File dir, ConfigFileGenerator generator) {
    this.dir = dir;
    this.generator = generator;
  }

  @Override
  public void localizeConfigFiles(InputFlowletSpecification spec) {
    try {
      if (!dir.exists()) {
        dir.mkdir();
      }

      File outputSpec = new File(dir, "output_spec.cfg");
      File packetSchema = new File(dir, "packet_schema.txt");
      File ifresXml = new File(dir, "ifres.xml");
      Map.Entry<String, String> ifqContent = generator.generateHostIfq().entrySet().iterator().next();
      String ifqFile = String.format("%s.ifq", ifqContent.getKey());
      File hostIfq = new File(dir, ifqFile);
      Files.write(generator.generateOutputSpec(spec), outputSpec, Charset.defaultCharset());
      Files.write(generator.generatePacketSchema(spec), packetSchema, Charset.defaultCharset());
      Files.write(generator.generateIfresXML(), ifresXml, Charset.defaultCharset());
      Files.write(ifqContent.getValue(), hostIfq, Charset.defaultCharset());
      Map<String, String> gsqlFiles = generator.generateGSQLFiles(spec);
      for (Map.Entry<String, String> gsqlFile : gsqlFiles.entrySet()) {
        String fileName = String.format("%s.gsql", gsqlFile.getKey());
        File file = new File(dir, fileName);
        Files.write(gsqlFile.getValue(), file, Charset.defaultCharset());
      }
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
    }
  }
}
