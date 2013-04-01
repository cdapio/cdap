package com.continuuity.performance.util;

import com.continuuity.api.common.Bytes;
import com.continuuity.performance.util.json.JSONOrderedObject;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.IOException;

public class MetricsHBaseUploader {
  private static String PERF_REPORT_TABLE = "perfreport";
  private static String PERF_CONF_TABLE = "perfreport";
  private static String DEFAULT_FAMILY = "fam";

  public static int uploadMetric(String reportURL,
                                 String benchRunId,
                                 String metricName,
                                 int minutes,
                                 double value)
    throws IOException {
    return upload(reportURL, PERF_REPORT_TABLE, benchRunId + "_" + metricName,
                  DEFAULT_FAMILY, minutes + "min", Bytes.toBytes(value));
  }
  public static int uploadConfig(String reportURL,
                                 String benchRunId,
                                 String configName,
                                 String value)
    throws IOException {
    return upload(reportURL, PERF_CONF_TABLE, benchRunId, DEFAULT_FAMILY, configName, Bytes.toBytes(value));
  }
  private static int upload(String reportURL, String table, String row, String family, String column,
                            byte[] value) throws IOException {
    String famCol = family + ":" + column;
    String fullURL = reportURL;
    if (reportURL.startsWith("http://")) {
      fullURL = reportURL;
    } else {
      fullURL = "http://" + reportURL;
    }
    HttpPut put = new HttpPut(String.format("%s/%s/%s/%s", fullURL, table, row, famCol));
    put.addHeader("Accept", "application/json");
    put.addHeader("Content-Type", "application/json");
    StringEntity entity = new StringEntity(genHBaseJSON(row, famCol, value), "UTF-8");
    entity.setContentType("application/json");
    put.setEntity(entity);
    HttpClient client = new DefaultHttpClient();
    HttpResponse response = client.execute(put);
    client.getConnectionManager().shutdown();
    return response.getStatusLine().getStatusCode();
  }
  private static String genHBaseJSON(String rowKey, String famCol, byte[] value) {
    JSONOrderedObject jsonCell = new JSONOrderedObject();
    jsonCell.put("@column", famCol);
    jsonCell.put("$", Base64.encodeBase64String(value));
    JSONOrderedObject jsonRow = new JSONOrderedObject();
    jsonRow.put("@key",rowKey);
    jsonRow.put("Cell", jsonCell);
    JSONOrderedObject jsonAllRows = new JSONOrderedObject();
    jsonAllRows.put("Row", jsonRow);
    return jsonAllRows.toJSONString();
  }
}
