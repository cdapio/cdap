package com.continuuity.jetstream.internal;

import com.continuuity.jetstream.api.GSSchema;
import com.continuuity.jetstream.api.PrimitiveType;
import com.continuuity.jetstream.gsflowlet.GSConfigFileGenerator;
import com.continuuity.jetstream.gsflowlet.GSFlowletSpecification;
import com.google.common.collect.Maps;
import com.google.common.io.CharStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Set;

/**
 * GSConfig File Content Generator.
 */
public class LocalGSConfigFileGenerator implements GSConfigFileGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(LocalGSConfigFileGenerator.class);
  private static final String OUTPUTSPEC_SUFFIX = ",stream,,,,,";
  private static final String NEWLINE = System.getProperty("line.separator");
  private final String hostname;

  public LocalGSConfigFileGenerator() {
    String host;
    try {
      host = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      host = "localhost";
    }
    this.hostname = host;
  }

  @Override
  public String generatePacketSchema(GSFlowletSpecification spec) {
    return createPacketSchema(spec.getGdatInputSchema());
  }

  @Override
  public String generateOutputSpec(GSFlowletSpecification spec) {
    return createOutputSpec(spec.getGSQL());
  }

  @Override
  public Map<String, String> generateGSQLFiles(GSFlowletSpecification spec) {
    return createGSQLFiles(spec.getGSQL());
  }

  @Override
  public Map<String, String> generateHostIfq() {
    Map<String, String> fileContent = Maps.newHashMap();
    String contents = createLocalHostIfq(hostname);
    fileContent.put(hostname, contents);
    return fileContent;
  }

  @Override
  public String generateIfresXML() {
    return createIfresXML(hostname);
  }

  private String createIfresXML(String hostname) {
    StringBuilder stringBuilder = new StringBuilder();
    //TODO: Ifres.xml won't be necessary after a default localhost fix in GSTool
    stringBuilder.append("<Resources>").append(NEWLINE).append("<Host Name='").append(hostname).append("'>")
      .append(NEWLINE);
    InputStream ifres = this.getClass().getClassLoader().getResourceAsStream("ifres.xml");
    try {
      stringBuilder.append(CharStreams.toString(new InputStreamReader(ifres, "UTF-8")));
      ifres.close();
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
    }
    return stringBuilder.toString();
  }

  private String createLocalHostIfq(String hostname) {
    return "default : Contains[InterfaceType, GDAT]";
  }

  private String createOutputSpec(Map<String, String> gsql) {
    StringBuilder stringBuilder = new StringBuilder();
    for (String outputStreamName : gsql.keySet()) {
      stringBuilder.append(outputStreamName).append(OUTPUTSPEC_SUFFIX).append(NEWLINE);
    }
    return stringBuilder.toString();
  }

  private String createPacketSchema(Map<String, GSSchema> schemaMap) {
    StringBuilder stringBuilder = new StringBuilder();
    for (Map.Entry<String, GSSchema> entry : schemaMap.entrySet()) {
      stringBuilder.append(createProtocol(entry.getKey(), entry.getValue()));
    }
    return stringBuilder.toString();
  }

  /**
   * Takes a name for the Schema and the GSSchema object to create the contents for packet_schema.txt file.
   * Sample file content :
   * PROTOCOL name {
   *   ullong timestamp get_gdat_ullong_pos1 (increasing);
   *   uint istream get_gdat_uint_pos2;
   * }
   */
  private String createProtocol(String name, GSSchema schema) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("PROTOCOL ").append(name).append(" {").append(NEWLINE);
    Set<String> increasingFields = schema.getIncreasingFields();
    Set<String> decreasingFields = schema.getDecreasingFields();
    int fieldCount = 1;
    for (Map.Entry<String, PrimitiveType> entry : schema.getFieldNames().entrySet()) {
      stringBuilder.append(entry.getValue().getType()).append(" ").append(entry.getKey()).append(" get_gdat_")
        .append(entry.getValue().getType()).append("_pos").append(fieldCount);
      if (increasingFields.contains(entry.getKey())) {
        stringBuilder.append(" (increasing)");
      } else if (decreasingFields.contains(entry.getKey())) {
        stringBuilder.append(" (decreasing)");
      }
      stringBuilder.append(";").append(NEWLINE);
      fieldCount++;
    }
    stringBuilder.append("}").append(NEWLINE);
    return stringBuilder.toString();
  }

  private String createGSQLContent(String name, String gsql) {
    StringBuilder stringBuilder = new StringBuilder();
    //TODO: Providing external visibility to all queries for now. Need to set this only for queries whose outputs
    //have corresponding process methods for better performance.
    String header = String.format("DEFINE { query_name '%s'; visibility 'external'; }", name);
    //Use default interface set
    stringBuilder.append(header).append(NEWLINE).append(gsql).append(NEWLINE);
    return stringBuilder.toString();
  }

  private Map<String, String> createGSQLFiles(Map<String, String> gsql) {
    Map<String, String> gsqlQueryMap = Maps.newHashMap();
    for (Map.Entry<String, String> sqlQuery : gsql.entrySet()) {
      gsqlQueryMap.put(sqlQuery.getKey(), createGSQLContent(sqlQuery.getKey(), sqlQuery.getValue()));
    }
    return gsqlQueryMap;
  }
}
