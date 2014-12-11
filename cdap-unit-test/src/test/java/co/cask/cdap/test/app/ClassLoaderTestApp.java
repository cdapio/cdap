/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.test.app;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.Tick;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.clearspring.analytics.util.Lists;
import com.google.common.base.Charsets;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * An application for testing bytecode generated classes ClassLoading behavior.
 * In specific, it tests DatumWriter in Flow and HttpHandler in Service.
 */
public class ClassLoaderTestApp extends AbstractApplication {

  @Override
  public void configure() {
    createDataset("records", KeyValueTable.class);
    addFlow(new BasicFlow());
    addService("RecordQuery", new RecordQueryHandler());
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

  public static final class BasicFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("BasicFlow")
        .setDescription("BasicFlow")
        .withFlowlets()
          .add(new Source())
          .add(new Sink())
        .connect()
          .from(new Source()).to(new Sink())
        .build();
    }
  }

  public static final class Source extends AbstractFlowlet {
    private OutputEmitter<List<Record>> output;
    private Random random = new Random();

    @Tick(delay = 1L, unit = TimeUnit.MILLISECONDS)
    public void generate() {
      // Emit PUBLIC or PRIVATE type randomly
      List<Record> records = Lists.newArrayList();
      for (int i = 0; i < 10; i++) {
        records.add(new Record(Record.Type.values()[random.nextInt(Record.Type.values().length)]));
      }
      output.emit(records);
    }
  }

  public static final class Sink extends AbstractFlowlet {

    @UseDataSet("records")
    private KeyValueTable records;

    @ProcessInput
    public void process(List<Record> inputs) {
      for (Record record : inputs) {
        records.increment(Bytes.toBytes(record.getType().name()), 1L);
      }
    }
  }

  public static final class RecordQueryHandler extends AbstractHttpServiceHandler {

    @UseDataSet("records")
    private KeyValueTable records;

    @GET
    @Path("/query")
    public void query(HttpServiceRequest request,
                      HttpServiceResponder responder, @QueryParam("type") Record record) {

      long count = Bytes.toLong(records.read(Bytes.toBytes(record.getType().name())));
      responder.sendString(200, Long.toString(count), Charsets.UTF_8);
    }
  }
}
