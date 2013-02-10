package com.continuuity.api;


import com.continuuity.WordCountApp;
import com.continuuity.api.io.ReflectionSchemaGenerator;
import com.continuuity.api.io.Schema;
import com.continuuity.api.io.SchemaTypeAdapter;
import com.continuuity.api.io.UnsupportedTypeException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ApplicationSpecificationTest {
  @Test
  public void testConfigureApplication() throws NoSuchMethodException, UnsupportedTypeException {
    ApplicationSpecification appSpec = new WordCountApp().configure();
    Gson gson = new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();

    ApplicationSpecification newSpec = gson.fromJson(gson.toJson(appSpec), ApplicationSpecification.class);

    Assert.assertEquals(1, newSpec.getDataSets().size());
    Assert.assertEquals(new ReflectionSchemaGenerator().generate(WordCountApp.MyRecord.class),
                          newSpec.getFlows().get("WordCountFlow").getFlowlets().get("Tokenizer")
                                 .getInputs().get("").iterator().next());
  }
}
