/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.explore.executor;

import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.explore.service.ExploreService;
import co.cask.cdap.explore.service.HandleNotFoundException;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.QueryHandle;
import co.cask.cdap.proto.QueryResult;
import co.cask.http.BodyProducer;
import com.google.gson.Gson;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;

/**
 * BodyProducer used for returning the results of a Query, chunk by chunk.
 */
final class QueryResultsBodyProducer extends BodyProducer {

  private static final Logger LOG = LoggerFactory.getLogger(QueryResultsBodyProducer.class);
  private static final Gson GSON = new Gson();

  private final ExploreService exploreService;
  private final QueryHandle handle;

  private final ChannelBuffer buffer;
  private final PrintWriter writer;

  private List<QueryResult> results;

  QueryResultsBodyProducer(ExploreService exploreService,
                           QueryHandle handle) throws HandleNotFoundException, SQLException, ExploreException {
    this.exploreService = exploreService;
    this.handle = handle;

    this.buffer = ChannelBuffers.dynamicBuffer();
    this.writer = new PrintWriter(new OutputStreamWriter(new ChannelBufferOutputStream(buffer),
                                                         StandardCharsets.UTF_8));
  }

  @Override
  public ChannelBuffer nextChunk() throws Exception {
    buffer.clear();

    if (results == null) {
      initialize();
    }

    if (results.isEmpty()) {
      return ChannelBuffers.EMPTY_BUFFER;
    }

    for (QueryResult result : results) {
      appendCSVRow(writer, result);
    }
    writer.flush();

    results = exploreService.nextResults(handle, AbstractExploreQueryExecutorHttpHandler.DOWNLOAD_FETCH_CHUNK_SIZE);
    return buffer;
  }

  private void initialize() throws HandleNotFoundException, SQLException, ExploreException {
    writer.println(getCSVHeaders(exploreService.getResultSchema(handle)));

    results = exploreService.previewResults(handle);
    if (results.isEmpty()) {
      results = exploreService.nextResults(handle, AbstractExploreQueryExecutorHttpHandler.DOWNLOAD_FETCH_CHUNK_SIZE);
    }
  }

  @Override
  public void finished() throws Exception {

  }

  @Override
  public void handleError(Throwable cause) {
    LOG.error("Received error while chunking query results.", cause);
  }

  private String getCSVHeaders(List<ColumnDesc> schema)
    throws HandleNotFoundException, SQLException, ExploreException {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (ColumnDesc columnDesc : schema) {
      if (first) {
        first = false;
      } else {
        sb.append(',');
      }
      sb.append(columnDesc.getName());
    }
    return sb.toString();
  }

  private void appendCSVRow(PrintWriter printWriter, QueryResult result)
    throws HandleNotFoundException, SQLException, ExploreException {
    boolean first = true;
    for (Object o : result.getColumns()) {
      if (first) {
        first = false;
      } else {
        printWriter.append(',');
      }
      // Using GSON toJson will serialize objects - in particular, strings will be quoted
      GSON.toJson(o, printWriter);
    }
    writer.println();
  }
}
