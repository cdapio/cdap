/* Copyright © 2016 Cask Data, Inc.
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
package co.cask.cdap.cask360;

import co.cask.cdap.api.annotation.HashPartition;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.cask360.Cask360Entity;
import co.cask.cdap.api.dataset.lib.cask360.Cask360Group;
import co.cask.cdap.api.dataset.lib.cask360.Cask360Table;
import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;

import com.google.gson.JsonSyntaxException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example flow for processing events from a {@link Stream} and writing them to
 * a {@link Cask360Table}.
 * <p>
 *
 * that consumes customer events from a Stream and stores Customer objects in
 * datastore. It has only two Flowlets: one consumes events from the Stream and
 * converts them into Customer objects, the other consumes these objects and
 * stores them in a DataSet.
 */
public class Cask360Flow extends AbstractFlow {
  private static final Logger LOG = LoggerFactory.getLogger(Cask360Group.class);

  /** Name of the Flowlet that reads from the stream */
  public static final String READER_FLOWLET_NAME = "streamReader";

  /** Name of the Flowlet that writes to the table */
  public static final String WRITER_FLOWLET_NAME = "tableWriter";

  /** Name of the Flow */
  @Property
  private String name;

  /** Description of the Flow */
  @Property
  private String desc;

  /** Name of the Stream this Flow will read from */
  @Property
  private String stream;

  /** Name of the Table this Flow will write to */
  @Property
  private String table;

  public Cask360Flow(String name, String desc, String stream, String table) {
    this.name = name;
    this.desc = desc;
    this.stream = stream;
    this.table = table;
  }

  @Override
  protected void configure() {
    setName(name);
    setDescription(desc);
    addFlowlet(READER_FLOWLET_NAME, new Cask360ReaderFlowlet());
    addFlowlet(WRITER_FLOWLET_NAME, new Cask360WriterFlowlet(table));
    connectStream(stream, READER_FLOWLET_NAME);
    connect(READER_FLOWLET_NAME, WRITER_FLOWLET_NAME);
  }

  /**
   * Flowlet that reads {@link StreamEvent}s, parses them into
   * {@link Cask360Entity}s and writes them to the next Flowlet.
   */
  public static class Cask360ReaderFlowlet extends AbstractFlowlet {

    /**
     * Output {@link Cask360Entity} objects to the {@link Cask360WriterFlowlet}
     */
    private OutputEmitter<Cask360Entity> out;

    @ProcessInput
    public void process(StreamEvent event) {

      // Get the body from the StreamEvent
      String body = Bytes.toString(event.getBody());
      LOG.info("Received event: " + body);
      try {
        // Attempt to parse Cask360Entity and emit to next Flowlet
        out.emit(Cask360Entity.fromString(body));
      } catch (IllegalArgumentException iae) {
        throw new RuntimeException("Invalid Cask360 Entity String: " + body, iae);
      } catch (JsonSyntaxException jse) {
        throw new RuntimeException("Invalid Cask360 Entity String: " + body, jse);
      }
    }
  }

  /**
   * Flowlet that accepts {@link Cask360Entity}s and writes them to the
   * specified {@link Cask360Table}.
   */
  public static class Cask360WriterFlowlet extends AbstractFlowlet {

    /** The name of the {@link Cask360Table} */
    @Property
    private String tableName;

    /**
     * The {@link Cask360Table} instance to write {@link Cask360Entity} objects
     */
    private Cask360Table table;

    public Cask360WriterFlowlet(String tableName) {
      this.tableName = tableName;
    }

    @Override
    public void initialize(FlowletContext context) throws Exception {
      super.initialize(context);
      table = context.getDataset(tableName);
    }

    @ProcessInput
    @HashPartition("id")
    public void process(Cask360Entity entity) {

      // Write the entity to the table
      table.write(entity);
    }
  }
}
