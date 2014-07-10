package com.continuuity.jetstream.internal;

import com.continuuity.jetstream.gsflowlet.GSConfigFileGenerator;
import com.continuuity.jetstream.gsflowlet.GSConfigFileLocalizer;
import com.continuuity.jetstream.gsflowlet.GSFlowletSpecification;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * Creates GS Config Files for a given {@link com.continuuity.jetstream.gsflowlet.GSFlowletSpecification}
 * in a given Directory location.
 */
public class LocalGSConfigFileLocalizer implements GSConfigFileLocalizer {

  private static final Logger LOG = LoggerFactory.getLogger(LocalGSConfigFileLocalizer.class);
  private File dir;
  private GSConfigFileGenerator generator;

  public LocalGSConfigFileLocalizer(File dir, GSConfigFileGenerator generator) {
    this.dir = dir;
    this.generator = generator;
  }

  @Override
  public void localizeGSConfigFiles(GSFlowletSpecification spec) {
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
