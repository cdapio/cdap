/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

import com.google.common.base.Charsets;
import io.cdap.cdap.api.annotation.UseDataSet;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.service.http.AbstractHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * An application for testing bytecode generated classes ClassLoading behavior.
 * In specific, it tests HttpHandler in Service.
 */
public class ClassLoaderTestApp extends AbstractApplication {

  @Override
  public void configure() {
    createDataset("records", KeyValueTable.class);
    addService("RecordHandler", new RecordHandler());
  }

  /**
   * A dummy record class for verification of DatumWriter generation.
   */
  public static final class Record {
    public enum Type {
      PUBLIC, PRIVATE
    }

    private Type type;

    public Record(Type type) {
      this.type = type;
    }

    public Record(String type) {
      this.type = Type.valueOf(type.toUpperCase());
    }

    public Type getType() {
      return type;
    }
  }

  public static final class RecordHandler extends AbstractHttpServiceHandler {

    @UseDataSet("records")
    private KeyValueTable records;

    @POST
    @Path("/increment/{type}")
    public void increment(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("type") String type) {
      records.increment(Bytes.toBytes(new Record(type).getType().name()), 1L);
      responder.sendStatus(200);
    }

    @GET
    @Path("/query")
    public void query(HttpServiceRequest request,
                      HttpServiceResponder responder, @QueryParam("type") Record record) {

      long count = Bytes.toLong(records.read(Bytes.toBytes(record.getType().name())));
      responder.sendString(200, Long.toString(count), Charsets.UTF_8);
    }
  }
}
