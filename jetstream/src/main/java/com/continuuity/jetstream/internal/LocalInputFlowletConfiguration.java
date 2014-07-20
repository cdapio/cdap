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

import com.continuuity.jetstream.flowlet.InputFlowletConfiguration;
import com.continuuity.jetstream.flowlet.InputFlowletSpecification;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

/**
 * Creates Config Files for a given {@link com.continuuity.jetstream.flowlet.InputFlowletSpecification}
 * in a given Directory location.
 */
public class LocalInputFlowletConfiguration implements InputFlowletConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(LocalInputFlowletConfiguration.class);
  private Location dir;

  public LocalInputFlowletConfiguration(Location dir) {
    this.dir = dir;
  }

  @Override
  public void createConfigFiles(InputFlowletSpecification spec) {
    try {
      if (!dir.exists()) {
        dir.mkdirs();
      }

      StreamConfigGenerator generator = new StreamConfigGenerator(spec);
      Location outputSpec = createFile(dir, "output_spec.cfg");
      Location packetSchema = createFile(dir, "packet_schema.txt");
      Location ifresXml = createFile(dir, "ifres.xml");
      Map.Entry<String, String> ifqContent = generator.generateHostIfq();
      String ifqFile = String.format("%s.ifq", ifqContent.getKey());
      Location hostIfq = createFile(dir, ifqFile);

      writeToLocation(outputSpec, generator.generateOutputSpec());
      writeToLocation(packetSchema, generator.generatePacketSchema());
      writeToLocation(ifresXml, generator.generateIfresXML());
      writeToLocation(hostIfq, generator.generateHostIfq().getValue());

      Map<String, String> gsqlFiles = generator.generateGSQLFiles();
      for (Map.Entry<String, String> gsqlFile : gsqlFiles.entrySet()) {
        String fileName = String.format("%s.gsql", gsqlFile.getKey());
        Location file = createFile(dir, fileName);
        writeToLocation(file, gsqlFile.getValue());
      }
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
    }
  }

  private Location createFile(Location dir, String name) throws IOException {
    Location file = dir.append(name);
    file.createNew();
    return file;
  }

  private void writeToLocation(Location loc, String content) throws IOException {
    OutputStream out = loc.getOutputStream();
    out.write(content.getBytes());
    out.close();
  }
}
