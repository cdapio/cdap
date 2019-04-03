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

package io.cdap.cdap.test.app;

import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.service.AbstractSystemService;
import io.cdap.cdap.api.service.http.AbstractSystemHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.FieldType;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.transaction.TransactionException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Application to test system application services.
 */
public class SystemTestApp extends AbstractApplication {
  public static final StructuredTableId TABLE_ID = new StructuredTableId("kv");
  public static final String SERVICE_NAME = "kv";
  public static final String KEY_COL = "key";
  public static final String VAL_COL = "val";

  @Override
  public void configure() {
    addService(new TableService());
  }

  /**
   * Service for accessing the table.
   */
  public static class TableService extends AbstractSystemService {

    @Override
    protected void configure() {
      setName(SERVICE_NAME);
      createTable(new StructuredTableSpecification.Builder()
                    .withId(TABLE_ID)
                    .withFields(new FieldType(KEY_COL, FieldType.Type.STRING),
                                new FieldType(VAL_COL, FieldType.Type.STRING))
                    .withPrimaryKeys(KEY_COL).build());
      addHandler(new TableHandler());
    }
  }

  /**
   * Service handler for accessing the table.
   */
  public static class TableHandler extends AbstractSystemHttpServiceHandler {

    @PUT
    @Path("{key}")
    public void put(HttpServiceRequest request, HttpServiceResponder responder,
                    @PathParam("key") String key) throws TransactionException {
      String val = StandardCharsets.UTF_8.decode(request.getContent()).toString();
      getContext().run(context -> {
        StructuredTable table = context.getTable(TABLE_ID);
        List<Field<?>> fields = new ArrayList<>(2);
        fields.add(Fields.stringField(KEY_COL, key));
        fields.add(Fields.stringField(VAL_COL, val));
        table.upsert(fields);
        responder.sendStatus(200);
      });
    }

    @GET
    @Path("{key}")
    public void get(HttpServiceRequest request, HttpServiceResponder responder,
                    @PathParam("key") String key) throws TransactionException {
      getContext().run(context -> {
        StructuredTable table = context.getTable(TABLE_ID);
        List<Field<?>> keyField = Collections.singletonList(Fields.stringField(KEY_COL, key));
        Optional<StructuredRow> row = table.read(keyField);
        if (!row.isPresent()) {
          responder.sendStatus(404);
        } else {
          responder.sendString(row.get().getString(VAL_COL));
        }
      });
    }

  }
}
