package com.continuuity.api;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.DataSet;
import com.continuuity.api.annotation.Output;
import com.continuuity.api.annotation.Process;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.io.Schema;
import com.continuuity.api.io.SchemaTypeAdapter;
import com.continuuity.api.io.UnsupportedTypeException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class ApplicationSpecificationTest {

  public static final class MyRecord {
    private String title;
    private String text;

    public MyRecord setTitle(String title) {
      this.title = title;
      return this;
    }

    public MyRecord setText(String text) {
      this.text = text;
      return this;
    }

    public String getTitle() {
      return title;
    }

    public String getText() {
      return text;
    }
  }

  public static class StreamSucker extends AbstractFlowlet {
    private OutputEmitter<MyRecord> output;

    public void process(StreamEvent event) throws CharacterCodingException {
      ByteBuffer buf = event.getBody();
      output.emit(new MyRecord()
                    .setTitle(event.getHeaders().get("title"))
                    .setText(buf == null ? null : Charset.forName("UTF-8").newDecoder().decode(buf).toString()));
    }
  }

  public static class TokenizerParent extends AbstractFlowlet {
    @Output("list")
    private OutputEmitter<List<String>> outputList;

    @Process("bar")
    public void bar(String str) {

    }
  }

  public static class Tokenizer extends TokenizerParent {
    @Output("map")
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
    @DataSet("mydataset")
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
      this.counters.stage(new KeyValueTable.IncrementKey(token.getBytes(Charset.forName("UTF-8"))));
    }
  }


  public static class WordCountFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.builder()
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

  public static class WordCountApp implements Application {

    @Override
    public ApplicationSpecification configure() {
      return ApplicationSpecification.builder()
        .setName("WordCountApp")
        .setDescription("Application for counting words")
        .withStreams().add(new Stream("text"))
        .withDataSets().add(new KeyValueTable("mydataset"))
        .withFlows().add(new WordCountFlow())
        .noProcedure().build();
    }
  }

  @Test
  public void testConfigureApplication() throws NoSuchMethodException, UnsupportedTypeException {
    ApplicationSpecification appSpec = new WordCountApp().configure();

    Gson gson = new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();

    // TODO: Do actual Assert
    System.out.println(gson.toJson(appSpec));
  }
}
