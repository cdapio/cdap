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

package io.cdap.cdap.test;

import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.FieldType;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.test.app.SystemTestApp;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Tests for system app.
 */
public class SystemAppTestBaseTest extends SystemAppTestBase {

  @Test
  public void testTableOperations() throws Exception {
    StructuredTableAdmin tableAdmin = getStructuredTableAdmin();
    StructuredTableId id = new StructuredTableId("t0");
    Assert.assertNull(tableAdmin.getSpecification(id));

    String keyCol = "key";
    String valCol = "val";
    tableAdmin.create(new StructuredTableSpecification.Builder()
                        .withId(id)
                        .withFields(new FieldType(keyCol, FieldType.Type.STRING),
                                    new FieldType(valCol, FieldType.Type.STRING))
                        .withPrimaryKeys(keyCol)
                        .build());

    try {
      TransactionRunner transactionRunner = getTransactionRunner();

      String key = "k0";
      String val = "v0";
      transactionRunner.run(context -> {
        StructuredTable table = context.getTable(id);
        List<Field<?>> fields = new ArrayList<>();
        fields.add(Fields.stringField(keyCol, key));

        Optional<StructuredRow> row = table.read(fields);
        Assert.assertFalse(row.isPresent());

        fields.add(Fields.stringField(valCol, val));
        table.upsert(fields);
      });

      transactionRunner.run(context -> {
        StructuredTable table = context.getTable(id);
        List<Field<?>> keyField = Collections.singletonList(Fields.stringField(keyCol, key));
        Optional<StructuredRow> row = table.read(keyField);
        Assert.assertTrue(row.isPresent());
        Assert.assertEquals(val, row.get().getString(valCol));
      });
    } finally {
      tableAdmin.drop(id);
    }
  }

  @Test
  public void testSystemServiceInUserNamespaceFails() {
    try {
      deployApplication(NamespaceId.DEFAULT, SystemTestApp.class);
      Assert.fail("Should not have been able to deploy a system service in a user namespace.");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testSystemService() throws Exception {
    ApplicationManager applicationManager = deployApplication(NamespaceId.SYSTEM, SystemTestApp.class);

    ServiceManager serviceManager = applicationManager.getServiceManager(SystemTestApp.SERVICE_NAME);
    serviceManager.startAndWaitForRun(ProgramRunStatus.RUNNING, 2, TimeUnit.MINUTES);
    URI serviceURI = serviceManager.getServiceURL(1, TimeUnit.MINUTES).toURI();

    String key = "k0";
    String val = "v0";
    URL url = serviceURI.resolve(key).toURL();

    HttpRequest request = HttpRequest.get(url).build();
    DefaultHttpRequestConfig requestConfig = new DefaultHttpRequestConfig(false);
    HttpResponse response = HttpRequests.execute(request, requestConfig);
    Assert.assertEquals(404, response.getResponseCode());

    request = HttpRequest.put(url).withBody(val).build();
    response = HttpRequests.execute(request, requestConfig);
    Assert.assertEquals(200, response.getResponseCode());

    request = HttpRequest.get(url).build();
    response = HttpRequests.execute(request, requestConfig);
    Assert.assertEquals(val, response.getResponseBodyAsString());
  }
}
