/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Output;
import com.continuuity.api.annotation.Process;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.List;
import java.util.Map;

/**
 * This is a sample word count app that is used in testing in
 * many places.
 */
public class WordCountApp implements Application {
  /**
   * Configures the {@link com.continuuity.api.Application} by returning an
   * {@link com.continuuity.api.ApplicationSpecification}
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
      .noProcedure().build();
  }

  public static interface MyRecord {
    String getTitle();

    String getText();

    boolean isExpired();
  }

  public static final class MyRecordImpl implements MyRecord {

    private final String title;
    private final String text;
    private final boolean expired;

    public MyRecordImpl(String title, String text, boolean expired) {
      this.title = title;
      this.text = text;
      this.expired = expired;
    }

    @Override
    public String getTitle() {
      return title;
    }

    @Override
    public String getText() {
      return text;
    }

    @Override
    public boolean isExpired() {
      return expired;
    }
  }

  public static class WordCountFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("WordCountFlow")
        .setDescription("Flow for counting words")
        .withFlowlets().add(new StreamSucker()).apply()
        .add(new Tokenizer()).apply()
        .add(new CountByField()).apply()
        .connect().from(new Stream("text")).to(new StreamSucker())
        .from(new StreamSucker()).to(new Tokenizer())
        .from(new Tokenizer()).to(new CountByField())
        .build();
    }
  }

  public static class StreamSucker extends AbstractFlowlet {
    private OutputEmitter<MyRecord> output;

    public void process(StreamEvent event, InputContext context) throws CharacterCodingException {
      if (!"text".equals(context.getName())) {
        return;
      }
      ByteBuffer buf = event.getBody();
      output.emit(new MyRecordImpl(
        event.getHeaders().get("title"),
        buf == null ? null : Charsets.UTF_8.newDecoder().decode(buf).toString(),
        false));
    }
  }

  public static class TokenizerParent extends AbstractFlowlet {
    @Output("mylist")
    private OutputEmitter<List<String>> outputList;

    @com.continuuity.api.annotation.Process("bar")
    public void bar(String str) {

    }
  }

  public static class Tokenizer extends TokenizerParent {
    @Output("field")
    private OutputEmitter<Map<String, String>> outputMap;

    @Process
    public void foo(MyRecord data) {
      tokenize(data.getTitle(), "title");
      tokenize(data.getText(), "title");
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

  public static class CountByField extends AbstractFlowlet {
    @UseDataSet("mydataset")
    private KeyValueTable counters;

    @Process("field")
    public void process(Map<String, String> fieldToken) {
      String token = fieldToken.get("word");
      if (token == null) {
        return;
      }
      String field = fieldToken.get("field");
      if (field != null) {
        token = field + ":" + token;
      }
      this.counters.stage(new KeyValueTable.IncrementKey(token.getBytes(Charsets.UTF_8)));
    }
  }
}
