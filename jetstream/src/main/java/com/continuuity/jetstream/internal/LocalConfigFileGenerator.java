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

import com.continuuity.jetstream.api.StreamSchema;
import com.continuuity.jetstream.api.PrimitiveType;
import com.continuuity.jetstream.flowlet.ConfigFileGenerator;
import com.continuuity.jetstream.flowlet.InputFlowletSpecification;
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
 * Config File Content Generator.
 */
public class LocalConfigFileGenerator implements ConfigFileGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(LocalConfigFileGenerator.class);
  private static final String OUTPUTSPEC_SUFFIX = ",stream,,,,,";
  private static final String NEWLINE = System.getProperty("line.separator");
  private final String hostname;

  public LocalConfigFileGenerator() {
    String host;
    try {
      host = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      host = "localhost";
    }
    this.hostname = host;
  }

  @Override
  public String generatePacketSchema(InputFlowletSpecification spec) {
    return createPacketSchema(spec.getGDATInputSchema());
  }

  @Override
  public String generateOutputSpec(InputFlowletSpecification spec) {
    return createOutputSpec(spec.getGSQL());
  }

  @Override
  public Map<String, String> generateGSQLFiles(InputFlowletSpecification spec) {
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

  private String createPacketSchema(Map<String, StreamSchema> schemaMap) {
    StringBuilder stringBuilder = new StringBuilder();
    for (Map.Entry<String, StreamSchema> entry : schemaMap.entrySet()) {
      stringBuilder.append(createProtocol(entry.getKey(), entry.getValue()));
    }
    return stringBuilder.toString();
  }

  /**
   * Takes a name for the Schema and the StreamSchema object to create the contents for packet_schema.txt file.
   * Sample file content :
   * PROTOCOL name {
   *   ullong timestamp get_gdat_ullong_pos1 (increasing);
   *   uint istream get_gdat_uint_pos2;
   * }
   */
  private String createProtocol(String name, StreamSchema schema) {
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
