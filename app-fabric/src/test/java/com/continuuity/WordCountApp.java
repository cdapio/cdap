/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.Output;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.Property;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.Callback;
import com.continuuity.api.flow.flowlet.FailurePolicy;
import com.continuuity.api.flow.flowlet.FailureReason;
import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.mapreduce.AbstractMapReduce;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.api.metrics.Metrics;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
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
 * many places.
 */
public class WordCountApp implements Application {

  private static final Logger LOG = LoggerFactory.getLogger(WordCountApp.class);

  /**
   * Configures the {@link com.continuuity.api.Application} by returning an
   * {@link com.continuuity.api.ApplicationSpecification}.
   *
   * @return An instance of {@code ApplicationSpecification}.
   */
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("WordCountApp")
      .setDescription("Application for counting words")
      .withStreams().add(new Stream("text"))
      .withDataSets().add(new KeyValueTable("mydataset"))
      .withFlows().add(new WordCountFlow())
      .withProcedures().add(new WordFrequency("word"))
      .withMapReduce().add(new VoidMapReduceJob())
      .noWorkflow()
      .build();
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
    public MapReduceSpecification configure() {
      return MapReduceSpecification.Builder.with()
        .setName("VoidMapReduceJob")
        .setDescription("Mapreduce that does nothing (and actually doesn't run) - it is here for testing MDS")
        .build();
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
}
