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

package co.cask.cdap;

import co.cask.cdap.api.annotation.Handle;
import co.cask.cdap.api.annotation.Output;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
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
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.worker.AbstractWorker;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * This is a sample word count app that is used in testing in
 * many places. It has one of each program type, most of which don't do anything.
 */
public class BloatedWordCountApp extends AbstractApplication {

  private static final Logger LOG = LoggerFactory.getLogger(BloatedWordCountApp.class);

  @Override
  public void configure() {
    setName("WordCountApp");
    setDescription("Application for counting words");
    addStream(new Stream("text"));
    createDataset("mydataset", KeyValueTable.class);
    addFlow(new WordCountFlow());
    addProcedure(new WordFrequency("word"));
    addMapReduce(new VoidMapReduceJob());
    addService(new NoopService());
    addSpark(new SparklingNothing());
    addWorker(new LazyGuy());
    addWorkflow(new SingleStep());
  }

  /**
   *
   */
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
//  @Async
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

  /**
   *
   */
  public static class WordFrequency extends AbstractProcedure {
    @UseDataSet("mydataset")
    private KeyValueTable counters;

    @Property
    private final String argumentName;

    public WordFrequency(String argumentName) {
      this.argumentName = argumentName;
    }

    @Handle("wordfreq")
    public void handle(ProcedureRequest request, ProcedureResponder responder) throws IOException {
      String word = request.getArgument(argumentName);
      Map<String, Long> result = ImmutableMap.of(word,
        Longs.fromByteArray(this.counters.read(word.getBytes(Charsets.UTF_8))));
      responder.sendJson(result);
    }
  }

  public static class NoopService extends AbstractService {
    @Override
    protected void configure() {
      setName("NoopService");
      setDescription("Dummy Service");
      addHandler(new DummyHandler());
    }
  }

  private static class DummyHandler extends AbstractHttpServiceHandler {
  }

  private static class SparklingNothing extends AbstractSpark {
    @Override
    protected void configure() {
      setDescription("Spark program that does nothing");
      setMainClass(this.getClass());
    }
    public static void main(String[] args) {
    }

  }

  private static class LazyGuy extends AbstractWorker {
    @Override
    protected void configure() {
      setDescription("nothing to describe");
    }

    @Override
    public void run() {
      // do nothing
    }
  }

  private static class SingleStep extends AbstractWorkflow {
    @Override
    public void configure() {
      addSpark("SparklingNothing");
    }
  }
}
