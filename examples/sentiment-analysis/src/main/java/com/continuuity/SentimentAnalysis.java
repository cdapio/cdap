package com.continuuity;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.ResourceSpecification;
import com.continuuity.api.annotation.Batch;
import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.Output;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.table.Get;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Row;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.api.flow.flowlet.FlowletSpecification;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.metrics.Metrics;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.flow.flowlet.ExternalProgramFlowlet;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;

/**
 *
 */
public class SentimentAnalysis implements Application {

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("sentiment")
      .setDescription("Sentiment Analysis")
      .withStreams()
        .add(new Stream("text"))
      .withDataSets()
        .add(new Table("sentiments"))
      .withFlows()
        .add(new SentimentAnalysisFlow())
      .withProcedures()
        .add(new SentimentAnalysisProcedure())
      .noMapReduce()
      .noWorkflow()
      .build();
  }

  /**
   *
   */
  public static class SentimentAnalysisFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("analysis")
        .setDescription("Analysis of text to generate sentiments")
        .withFlowlets()
          .add(new Normalization())
          .add(new Analyze())
          .add(new Update())
        .connect()
          .fromStream("text").to(new Normalization())
          .from(new Normalization()).to(new Analyze())
          .from(new Analyze()).to(new Update())
        .build();
    }
  }

  /**
   *
   */
  public static class Normalization extends AbstractFlowlet {
    private static final Logger LOG = LoggerFactory.getLogger(Normalization.class);

    private OutputEmitter<String> out;

    Metrics metrics;

    @ProcessInput
    public void process(StreamEvent event) {
      String text = Bytes.toString(Bytes.toBytes(event.getBody()));
      if (text != null) {
        metrics.count("data.processed.size", text.length());
        out.emit(text);
      } else {
        metrics.count("data.ignored.text", 1);
      }
    }
  }

  /**
   *
   */
  public static class Analyze extends ExternalProgramFlowlet<String, String> {
    private static final Logger LOG = LoggerFactory.getLogger(Normalization.class);

    @Output("sentiments")
    private OutputEmitter<String> sentiment;

    /**
     * This method will be called at Flowlet initialization time.
     *
     * @param context The {@link com.continuuity.api.flow.flowlet.FlowletContext} for this Flowlet.
     * @return An {@link com.continuuity.flow.flowlet.ExternalProgramFlowlet.ExternalProgram} to specify
     * properties of the external program to process input.
     */
    @Override
    protected ExternalProgram init(FlowletContext context) {
      URL file = this.getClass().getClassLoader().getResource("sentiment/score-sentence");
      if (file != null) {
        File bash = new File("/bin/bash");
        if (bash.exists()) {
          return new ExternalProgram(bash, file.getFile());
        } else {
          bash = new File("/usr/bin/bash");
          if (bash.exists()) {
            return new ExternalProgram(bash, file.getFile());
          }
        }
      }
      throw new RuntimeException("Unable to start process");
    }

    /**
     * This method will be called for each input event to transform the given input into string before sending to
     * external program for processing.
     *
     * @param input The input event.
     * @return A UTF-8 encoded string of the input, or {@code null} if to skip this input.
     */
    @Override
    protected String encode(String input) {
      return input;
    }

    /**
     * This method will be called when the external program returns the result. Child class can do it's own processing
     * in this method or could return an object of type {@code OUT} for emitting to next flowlet with the
     * {@link com.continuuity.api.flow.flowlet.OutputEmitter} returned by {@link #getOutputEmitter()}.
     *
     * @param result The result from the external program.
     * @return The output to emit or {@code null} if nothing to emit.
     */
    @Override
    protected String processResult(String result) {
      return result;
    }

    /**
     * Child class can override this method to return an OutputEmitter for writing data to the next flowlet.
     *
     * @return An {@link com.continuuity.api.flow.flowlet.OutputEmitter} for type {@code OUT}, or {@code null} if
     * this flowlet doesn't have output.
     */
    @Override
    protected OutputEmitter<String> getOutputEmitter() {
      return sentiment;
    }
  }

  /**
   *
   */
  public static class Update extends AbstractFlowlet {
    private static final Logger LOG = LoggerFactory.getLogger(Normalization.class);

    @UseDataSet("sentiments")
    private Table sentiments;

    Metrics metrics;

    @Batch(10)
    @ProcessInput("sentiments")
    public void process(Iterator<String> sentimentItr) {
      while(sentimentItr.hasNext()) {
        String sentiment = sentimentItr.next();
        metrics.count("sentiment." + sentiment, 1);
        LOG.info("Sentiment {}", sentiment);
        sentiments.increment(new Increment("aggregate", sentiment, 1));
      }
    }

    @Override
    public FlowletSpecification configure() {
      return FlowletSpecification.Builder.with()
        .setName("update")
        .setDescription("Updates the sentiment counts")
        .withResources(ResourceSpecification.BASIC)
        .build();
    }
  }

  /**
   *
   */
  public static class SentimentAnalysisProcedure extends AbstractProcedure {
    private static final Logger LOG = LoggerFactory.getLogger(SentimentAnalysisProcedure.class);

    @UseDataSet("sentiments")
    private Table sentiments;

    @Handle("aggregates")
    public void sentimentAggregates(ProcedureRequest request, ProcedureResponder response) throws Exception {
      Row row = sentiments.get(new Get("aggregate"));
      Map<byte[], byte[]> result = row.getColumns();
      if (result == null) {
        response.error(ProcedureResponse.Code.FAILURE, "No sentiments processed.");
        return;
      }
      Map<String, Long> resp = Maps.newHashMap();
      for (Map.Entry<byte[], byte[]> entry : result.entrySet()) {
        resp.put(Bytes.toString(entry.getKey()), Bytes.toLong(entry.getValue()));
      }
      response.sendJson(ProcedureResponse.Code.SUCCESS, resp);
    }

    @Override
    public ProcedureSpecification configure() {
      return ProcedureSpecification.Builder.with()
        .setName("sentiment-query")
        .setDescription("Sentiments Procedure")
        .withResources(ResourceSpecification.BASIC)
        .build();
    }
  }
}
