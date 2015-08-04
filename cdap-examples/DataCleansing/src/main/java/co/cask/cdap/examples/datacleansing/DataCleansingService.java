/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.examples.datacleansing;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionOutput;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.Service;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * A {@link Service} to write to PartitionedFileSet.
 */
public class DataCleansingService extends AbstractService {

  public static final String NAME = "DataCleansingService";

  @Override
  protected void configure() {
    setName(NAME);
    setDescription("A service to ingest data into the rawRecords partitioned file set.");
    addHandler(new RecordsHandler());
  }

  /**
   * A handler that allows writing to the 'rawRecords' PartitionedFileSet.
   */
  @Path("/v1")
  public static class RecordsHandler extends AbstractHttpServiceHandler {

    @SuppressWarnings("unused")
    @UseDataSet(DataCleansing.RAW_RECORDS)
    private PartitionedFileSet rawRecords;

    @SuppressWarnings("unused")
    @UseDataSet(DataCleansing.CLEAN_RECORDS)
    private PartitionedFileSet cleanRecords;

    @POST
    @Path("/records/raw")
    public void write(HttpServiceRequest request, HttpServiceResponder responder) {
      PartitionKey key = PartitionKey.builder().addLongField("time", System.currentTimeMillis()).build();
      PartitionOutput partitionOutput = rawRecords.getPartitionOutput(key);
      Location location = partitionOutput.getLocation();

      try (WritableByteChannel channel = Channels.newChannel(location.getOutputStream())) {
        channel.write(request.getContent());
        partitionOutput.addPartition();
      } catch (IOException e) {
        responder.sendError(400, String.format("Unable to write path '%s'", location));
        return;
      }
      responder.sendStatus(200);
    }
  }
}
