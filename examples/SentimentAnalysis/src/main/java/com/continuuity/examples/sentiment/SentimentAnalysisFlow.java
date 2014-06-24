/**
 * Copyright 2013-2014 Continuuity, Inc.
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
package com.continuuity.examples.sentiment;

import com.continuuity.api.ResourceSpecification;
import com.continuuity.api.annotation.Batch;
import com.continuuity.api.annotation.Output;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.lib.TimeseriesTable;
import com.continuuity.api.dataset.table.Increment;
import com.continuuity.api.dataset.table.Table;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.api.flow.flowlet.FlowletSpecification;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.metrics.Metrics;
import com.continuuity.flow.flowlet.ExternalProgramFlowlet;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
 * Flow for sentiment analysis.
 */
public class SentimentAnalysisFlow implements Flow {

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
        .fromStream("sentence").to(new Normalization())
        .from(new Normalization()).to(new Analyze())
        .from(new Analyze()).to(new Update())
      .build();
  }

  /**
   * Normalizes the sentences.
   */
  public static class Normalization extends AbstractFlowlet {
    private static final Logger LOG = LoggerFactory.getLogger(Normalization.class);

    /**
     * Emitter for emitting sentences from this Flowlet.
     */
    private OutputEmitter<String> out;

    /**
     * Handler to emit metrics.
     */
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
   * Analyzes the sentences by passing the sentence to NLTK based sentiment analyzer
   * written in Python.
   */
  public static class Analyze extends ExternalProgramFlowlet<String, String> {
    private static final Logger LOG = LoggerFactory.getLogger(Normalization.class);

    @Output("sentiments")
    private OutputEmitter<String> sentiment;

    private File workDir;

    /**
     * This method will be called at Flowlet initialization time.
     *
     * @param context The {@link com.continuuity.api.flow.flowlet.FlowletContext} for this Flowlet.
     * @return An {@link com.continuuity.flow.flowlet.ExternalProgramFlowlet.ExternalProgram} to specify
     * properties of the external program to process input.
     */
    @Override
    protected ExternalProgram init(FlowletContext context) {
      try {
        InputStream in = this.getClass().getClassLoader().getResourceAsStream("sentiment-process.zip");

        if (in != null) {
          workDir = new File("work");
          Unzipper.unzip(in, workDir);

          File bash = new File("/bin/bash");
          File program = new File(workDir, "sentiment/score-sentence");

          if (bash.exists()) {
            return new ExternalProgram(bash, program.getAbsolutePath());
          } else {
            bash = new File("/usr/bin/bash");
            if (bash.exists()) {
              return new ExternalProgram(bash, program.getAbsolutePath());
            }
          }
        }

        throw new RuntimeException("Unable to start process");
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
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
     * This method will be called when the external program returns the result. Child class can do its own processing
     * in this method or could return an object of type {@code OUT} for emitting to next Flowlet with the
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
     * Child class can override this method to return an OutputEmitter for writing data to the next Flowlet.
     *
     * @return An {@link com.continuuity.api.flow.flowlet.OutputEmitter} for type {@code OUT}, or {@code null} if
     * this flowlet doesn't have output.
     */
    @Override
    protected OutputEmitter<String> getOutputEmitter() {
      return sentiment;
    }

    @Override
    protected void finish() {
      try {
        LOG.info("Deleting work dir {}", workDir);
        FileUtils.deleteDirectory(workDir);
      } catch (IOException e) {
        LOG.error("Could not delete work dir {}", workDir);
        throw Throwables.propagate(e);
      }
    }
  }
  /**
   * Updates the timeseries table with sentiments received.
   */
  public static class Update extends AbstractFlowlet {
    private static final Logger LOG = LoggerFactory.getLogger(Normalization.class);

    @UseDataSet("sentiments")
    private Table sentiments;

    @UseDataSet("text-sentiments")
    private TimeseriesTable textSentiments;

    Metrics metrics;

    @Batch(10)
    @ProcessInput("sentiments")
    public void process(Iterator<String> sentimentItr) {
      while (sentimentItr.hasNext()) {
        String text = sentimentItr.next();
        Iterable<String> parts = Splitter.on("---").split(text);
        if (Iterables.size(parts) == 2) {
          String sentence = Iterables.get(parts, 0);
          String sentiment = Iterables.get(parts, 1);
          metrics.count("sentiment." + sentiment, 1);
          LOG.info("Sentence = {}, Sentiment = {}", sentence, sentiment);
          sentiments.increment(new Increment("aggregate", sentiment, 1));
          textSentiments.write(new TimeseriesTable.Entry(sentiment.getBytes(Charsets.UTF_8),
                                                         sentence.getBytes(Charsets.UTF_8),
                                                         System.currentTimeMillis()));
        } else {
          metrics.count("data.ignored.sentiments", 1);
        }
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
}
