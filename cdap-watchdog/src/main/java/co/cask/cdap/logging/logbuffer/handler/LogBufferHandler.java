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

package co.cask.cdap.logging.logbuffer.handler;

import co.cask.cdap.common.io.ByteBuffers;
import co.cask.cdap.logging.logbuffer.ConcurrentLogBufferWriter;
import co.cask.cdap.logging.logbuffer.LogBufferRequest;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * A netty http handler to receive logs from clients.
 */
@Path("/v1/logs")
public class LogBufferHandler extends AbstractHttpHandler {
  private final ConcurrentLogBufferWriter writer;

  public LogBufferHandler(ConcurrentLogBufferWriter writer) {
    this.writer = writer;
  }

  @POST
  @Path("/partitions/{partition-id}/publish")
  public void process(FullHttpRequest request, HttpResponder responder,
                      @PathParam("partition-id") int partitionId) throws Exception {
    LogBufferRequest bufferRequest = createLogBufferRequest(request, partitionId);
    writer.process(bufferRequest);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Creates a {@link LogBufferRequest} instance based on the given {@link HttpRequest}.
   */
  @SuppressWarnings("unchecked")
  private LogBufferRequest createLogBufferRequest(FullHttpRequest request, int partitionId) throws Exception {
    Decoder decoder = DecoderFactory.get().directBinaryDecoder(new ByteBufInputStream(request.content()), null);
    DatumReader<List<ByteBuffer>> datumReader = new GenericDatumReader<>(Schema.createArray(Schema.create(Schema.Type
                                                                                                            .BYTES)));
    List<ByteBuffer> events = datumReader.read(null, decoder);
    //return new LogBufferRequest(partitionId, events);
    return new LogBufferRequest(partitionId,
                                events.stream().map(ByteBuffers::getByteArray).collect(Collectors.toList()));
  }
}
