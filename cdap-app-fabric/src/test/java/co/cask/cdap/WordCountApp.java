/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap;

import co.cask.cdap.api.annotation.Output;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.Callback;
import co.cask.cdap.api.flow.flowlet.FailurePolicy;
import co.cask.cdap.api.flow.flowlet.FailureReason;
import co.cask.cdap.api.flow.flowlet.InputContext;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Map;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * This is a sample word count app that is used in testing in
 * many places.
 */
public class WordCountApp extends AbstractApplication {

  private static final Logger LOG = LoggerFactory.getLogger(WordCountApp.class);

  @Override
  public void configure() {
    setName("WordCountApp");
    setDescription("Application for counting words");
    addStream(new Stream("text"));
    createDataset("mydataset", KeyValueTable.class);
    addFlow(new WordCountFlow());
    addService(new WordFrequencyService());
    addMapReduce(new VoidMapReduceJob());
  }

  public static final class MyRecord {

    private final String title;
    private final String text;
    private final boolean expired;

    public MyRecord(String title, String text, boolean expired) {
      this.title = title;
      this.text = text;
      this.expired = expired;
    }

    public String getTitle() {
      return title;
    }

    public String getText() {
      return text;
    }

    public boolean isExpired() {
      return expired;
    }
  }

  /**
   * Map reduce job to test MDS.
   */
  public static class VoidMapReduceJob extends AbstractMapReduce {

    @Override
    protected void configure() {
      setDescription("Mapreduce that does nothing (and actually doesn't run) - it is here for testing MDS");
    }
  }

  /**
   *
   */
  public static class WordCountFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("WordCountFlow")
        .setDescription("Flow for counting words")
        .withFlowlets().add("StreamSource", new StreamSucker())
                       .add(new Tokenizer())
                       .add(new CountByField("word", "field"))
        .connect().fromStream("text").to("StreamSource")
                  .from("StreamSource").to("Tokenizer")
                  .from("Tokenizer").to("CountByField")
        .build();
    }
  }

  /**
   *
   */
  public static class StreamSucker extends AbstractFlowlet {
    private OutputEmitter<MyRecord> output;
    private Metrics metrics;

    @ProcessInput
    public void process(StreamEvent event, InputContext context) throws CharacterCodingException {
      if (!"text".equals(context.getOrigin())) {
        return;
      }

      metrics.count("stream.event", 1);

      ByteBuffer buf = event.getBody();
      output.emit(new MyRecord(
        event.getHeaders().get("title"),
        buf == null ? null : Charsets.UTF_8.newDecoder().decode(buf).toString(),
        false));
    }
  }

  /**
   *
   */
  public static class Tokenizer extends AbstractFlowlet {
    @Output("field")
    private OutputEmitter<Map<String, String>> outputMap;

    @ProcessInput
    public void foo(MyRecord data) {
      tokenize(data.getTitle(), "title");
      tokenize(data.getText(), "text");
    }

    private void tokenize(String str, String field) {
      if (str == null) {
        return;
      }
      final String delimiters = "[ .-]";
      for (String token : str.split(delimiters)) {
        outputMap.emit(ImmutableMap.of("field", field, "word", token));
      }
    }
  }

  /**
   *
   */
  public static class CountByField extends AbstractFlowlet implements Callback {
    @UseDataSet("mydataset")
    private KeyValueTable counters;

    @Property
    private final String wordKey;

    @Property
    private final String fieldKey;

    @Property
    private final long increment = 1L;

    public CountByField(String wordKey, String fieldKey) {
      this.wordKey = wordKey;
      this.fieldKey = fieldKey;
    }

    @ProcessInput("field")
    public void process(Map<String, String> fieldToken) {
      LOG.info("process count by field: " + fieldToken);

      String token = fieldToken.get(wordKey);
      if (token == null) {
        return;
      }
      String field = fieldToken.get(fieldKey);
      if (field != null) {
        token = field + ":" + token;
      }

      this.counters.increment(token.getBytes(Charsets.UTF_8), increment);

      byte[] bytes = this.counters.read(token.getBytes(Charsets.UTF_8));
      LOG.info(token + " " + Longs.fromByteArray(bytes));
    }

    @Override
    public void onSuccess(@Nullable Object input, @Nullable InputContext inputContext) {
    }

    @Override
    public FailurePolicy onFailure(@Nullable Object input, @Nullable InputContext inputContext, FailureReason reason) {
      return FailurePolicy.RETRY;
    }
  }

  public static class WordFrequencyService extends AbstractService {
    @Override
    protected void configure() {
      addHandler(new WordFrequencyHandler());
    }
  }

  public static class WordFrequencyHandler extends AbstractHttpServiceHandler {

    @UseDataSet("mydataset")
    private KeyValueTable counters;

    @GET
    @Path("wordfreq/{word}")
    public void wordFrequency(HttpServiceRequest request, HttpServiceResponder responder,
                              @PathParam("word") String word) {
      byte[] value = counters.read(word);
      if (value == null) {
        responder.sendStatus(404);
        return;
      }
      responder.sendJson(ImmutableMap.of(word, Bytes.toLong(value)));
    }
  }
}
