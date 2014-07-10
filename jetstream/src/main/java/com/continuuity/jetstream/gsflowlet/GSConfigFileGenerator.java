package com.continuuity.jetstream.gsflowlet;

import java.util.Map;

/**
 * GS Configuration Content Generator.
 */
public interface GSConfigFileGenerator {

  public String generatePacketSchema(GSFlowletSpecification spec);

  public String generateOutputSpec(GSFlowletSpecification spec);

  public Map<String, String> generateGSQLFiles(GSFlowletSpecification spec);

  public Map<String, String> generateHostIfq();

  public String generateIfresXML();
}
