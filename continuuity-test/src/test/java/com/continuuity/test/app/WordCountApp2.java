/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.test.app;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.Output;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.data.stream.StreamBatchReadable;
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
import com.continuuity.api.mapreduce.MapReduceContext;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.api.metrics.Metrics;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Map;
import java.util.StringTokenizer;
import javax.annotation.Nullable;

/**
 * This is a sample word count app that is used in testing in
 * many places.
 */
public class WordCountApp2 implements Application {

  private static final Logger LOG = LoggerFactory.getLogger(WordCountApp2.class);

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
      .withStreams().add(new Stream("text2"))
      .withDataSets().add(new MyKeyValueTable("mydataset"))
                     .add(new MyKeyValueTable("totals"))
      .withFlows().add(new WordCountFlow())
      .withProcedures().add(new WordFrequency())
      .withMapReduce().add(new CountTotal())
                      .add(new CountFromStream())
      .noWorkflow()
      .build();
  }

  /**
   * Output object of stream source.
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

    @Override
    public String toString() {
      return "MyRecord{" +
        "title='" + title + '\'' +
        ", text='" + text + '\'' +
        ", expired=" + expired +
        '}';
    }
  }

  /**
   * Flow that counts words coming from stream source.
   */
  public static class WordCountFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("WordCountFlow")
        .setDescription("Flow for counting words")
        .withFlowlets().add(new StreamSource())
                       .add(new Tokenizer())
                       .add(new CountByField())
        .connect().fromStream("text2").to("StreamSource")
                  .from("StreamSource").to("Tokenizer")
                  .from("Tokenizer").to("CountByField")
        .build();
    }
  }

  /**
   * Stream source for word count flow.
   */
  public static final class StreamSource extends AbstractFlowlet {
    private OutputEmitter<MyRecord> output;
    private Metrics metrics;

    @ProcessInput
    public void process(StreamEvent event, InputContext context) throws CharacterCodingException {
      if (!"text2".equals(context.getOrigin())) {
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
   * Tokenizer for word count flow.
   */
  public static class Tokenizer extends AbstractFlowlet {
    @Output("field")
    private OutputEmitter<Map<String, String>> outputMap;

    private boolean error = true;

    @ProcessInput
    public void foo(MyRecord data) {
      tokenize(data.getTitle(), "title");
      tokenize(data.getText(), "text2");
      if (error) {
        error = false;
        throw new IllegalStateException(data.toString());
      }
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
   * Flow that counts words and stores them in a table.
   */
  public static class CountByField extends AbstractFlowlet implements Callback {
    @UseDataSet("mydataset")
    private MyKeyValueTable counters;

    @ProcessInput("field")
    public void process(Map<String, String> fieldToken) {

      String token = fieldToken.get("word");
      if (token == null) {
        return;
      }
      String field = fieldToken.get("field");
      if (field != null) {
        token = field + ":" + token;
      }

      this.counters.increment(token.getBytes(Charsets.UTF_8), 1L);
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
   * Procedure to query word counts.
   */
  public static class WordFrequency extends AbstractProcedure {
    @UseDataSet("mydataset")
    private MyKeyValueTable counters;

    @UseDataSet("totals")
    private MyKeyValueTable totals;

    @Handle("wordfreq")
    private void wordfreq(ProcedureRequest request, ProcedureResponder responder)
      throws IOException {
      String word = request.getArgument("word");
      Map<String, Long> result = ImmutableMap.of(word,
        Longs.fromByteArray(this.counters.read(word.getBytes(Charsets.UTF_8))));
      responder.sendJson(new ProcedureResponse(ProcedureResponse.Code.SUCCESS), result);
    }

    @Handle("total")
    private void total(ProcedureRequest request, ProcedureResponder responder) throws IOException {
      long result = Bytes.toLong(this.totals.read(Bytes.toBytes("total_words_count")));
      responder.sendJson(new ProcedureResponse(ProcedureResponse.Code.SUCCESS), result);
    }

    @Handle("stream_total")
    public void streamTotal(ProcedureRequest request, ProcedureResponder responder) throws IOException {
      long result = Bytes.toLong(this.totals.read(Bytes.toBytes("stream_total_words_count")));
      responder.sendJson(new ProcedureResponse(ProcedureResponse.Code.SUCCESS), result);
    }
  }

  /**
   * Map Reduce to count total of counts.
   */
  public static class CountTotal extends AbstractMapReduce {
    @Override
    public MapReduceSpecification configure() {
      return MapReduceSpecification.Builder.with()
        .setName("countTotal")
        .setDescription("Counts total words count")
        .useInputDataSet("mydataset")
        .useOutputDataSet("totals")
        .build();
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Job job = context.getHadoopJob();
      job.setMapperClass(MyMapper.class);
      job.setMapOutputKeyClass(BytesWritable.class);
      job.setMapOutputValueClass(LongWritable.class);
      job.setReducerClass(MyReducer.class);
    }

    /**
     * Mapper for map reduce job.
     */
    public static class MyMapper extends Mapper<byte[], byte[], BytesWritable, LongWritable> {
      @Override
      protected void map(byte[] key, byte[] value, Context context) throws IOException, InterruptedException {
        context.write(new BytesWritable(Bytes.toBytes("total")), new LongWritable(Bytes.toLong(value)));
      }
    }

    /**
     * Reducer for map reduce job.
     */
    public static class MyReducer extends Reducer<BytesWritable, LongWritable, byte[], byte[]> {
      @Override
      protected void reduce(BytesWritable key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {

        long total = 0;
        for (LongWritable longWritable : values) {
          total += longWritable.get();
        }

        context.write(Bytes.toBytes("total_words_count"), Bytes.toBytes(total));
      }
    }
  }

  /**
   * Performs word count from stream data directly.
   */
  public static final class CountFromStream extends AbstractMapReduce {

    @Override
    public MapReduceSpecification configure() {
      return MapReduceSpecification.Builder.with()
        .setName("countFromStream")
        .setDescription("Word count from stream")
        .useOutputDataSet("totals")
        .build();
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      context.setInput(new StreamBatchReadable("text2"), null);

      Job job = context.getHadoopJob();
      job.setMapperClass(StreamMapper.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(LongWritable.class);
      job.setReducerClass(StreamReducer.class);
    }

    /**
     * Mapper for the count from stream.
     */
    public static final class StreamMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

      private static final Text TOTAL = new Text("total");

      @Override
      protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        long total = 0;
        while (itr.hasMoreTokens()) {
          total++;
          itr.nextToken();
        }
        context.write(TOTAL, new LongWritable(total));
      }
    }

    /**
     * Reducer for the count from stream.
     */
    public static final class StreamReducer extends Reducer<Text, LongWritable, byte[], byte[]> {

      @Override
      protected void reduce(Text key, Iterable<LongWritable> values,
                            Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable val : values) {
          sum += val.get();
        }
        context.write(Bytes.toBytes("stream_total_words_count"), Bytes.toBytes(sum));
      }
    }
  }

}
