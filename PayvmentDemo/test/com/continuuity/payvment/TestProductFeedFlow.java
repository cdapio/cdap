package com.continuuity.payvment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.continuuity.api.data.OperationContext;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.omid.OmidTransactionalOperationExecutor;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.flow.FlowTestHelper;
import com.continuuity.flow.FlowTestHelper.TestFlowHandle;
import com.continuuity.flow.definition.impl.FlowStream;
import com.continuuity.flow.flowlet.internal.TupleSerializer;
import com.continuuity.payvment.util.Bytes;
import com.google.gson.Gson;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class TestProductFeedFlow {

  private static final Injector injector =
      Guice.createInjector(new DataFabricModules().getInMemoryModules());

  private static final OmidTransactionalOperationExecutor executor =
      (OmidTransactionalOperationExecutor)injector.getInstance(
          OperationExecutor.class);

  @SuppressWarnings("unused")
  private static final OVCTableHandle handle = executor.getTableHandle();
  
  private static CConfiguration conf;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = CConfiguration.create();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Test(timeout = 10000)
  public void testProductFeedFlow_Basic() throws Exception {
    
    // Instantiate product feed flow
    ProductFeedFlow productFeedFlow = new ProductFeedFlow();
    
    // Start the flow
    TestFlowHandle flowHandle =
        FlowTestHelper.startFlow(productFeedFlow, conf, executor);
    assertTrue(flowHandle.isSuccess());
    
    // Generate a single product event
    long now = System.currentTimeMillis();
    ProductMeta productMeta =
        new ProductMeta(1L, 2L, now, "category", "name", 3.5);
    String productMetaJson =
        ProductFeedParserFlowlet.generateProductMetaJson(productMeta);
    
    // Write json to input stream
    writeToStream(ProductFeedFlow.inputStream, Bytes.toBytes(productMetaJson));
    
    // Wait for parsing flowlet to process the tuple
    while (ProductFeedParserFlowlet.numProcessed < 1) {
      System.out.println("Waiting for parsing flowlet to process tuple");
      Thread.sleep(10);
    }
    
    // Wait for processor flowlet to process the tuple
    while (ProductFeedFlow.ProductProcessorFlowlet.numProcessed < 1) {
      System.out.println("Waiting for processor flowlet to process tuple");
      Thread.sleep(10);
    }
    
    // Wait for processor flowlet to process the tuple
    while (ProductFeedFlow.ProductActivityFeedUpdaterFlowlet.numProcessed < 1) {
      System.out.println("Waiting for updater flowlet to process tuple");
      Thread.sleep(10);
    }
    
    System.out.println("Tuple processed to the end!");

    // If we are here, flow ran successfully!
    assertTrue(FlowTestHelper.stopFlow(flowHandle));
    
    // Verify the product is stored
    
    // Verify activity feed entry has been generated
    
  }
  
  private void writeToStream(String streamName, byte[] bytes)
      throws OperationException {
    Map<String,String> headers = new HashMap<String,String>();
    TupleSerializer serializer = new TupleSerializer(false);
    Tuple tuple = new TupleBuilder()
        .set("headers", headers)
        .set("body", bytes)
        .create();
    executor.execute(OperationContext.DEFAULT,
        new QueueEnqueue(
            Bytes.toBytes(FlowStream.defaultInputURI(streamName).toString()),
            serializer.serialize(tuple)));
  }

  @Test
  public void testProductMetaSerialization() throws Exception {

    String exampleProductMetaJson =
        "{\"@id\":\"6709879\"," +
        "\"category\":\"Dresses\"," +
        "\"name\":\"Sleeveless Black Sequin Dress\"," +
        "\"last_modified\":\"1348074421000\"," +
        "\"store_id\":\"164341\"," +
        "\"score\":\"1.3\"}";
    
    String processedJsonString =
        ProductFeedParserFlowlet.preProcessSocialActionJSON(
            exampleProductMetaJson);
    
    Gson gson = new Gson();
    ProductMeta productMeta =
        gson.fromJson(processedJsonString, ProductMeta.class);
    
    assertEquals(new Long(6709879), productMeta.product_id);
    assertEquals(new Long(164341), productMeta.store_id);
    assertEquals(new Long(1348074421000L), productMeta.date);
    assertEquals("Dresses", productMeta.category);
    assertEquals("Sleeveless Black Sequin Dress", productMeta.name);
    assertEquals(new Double(1.3), productMeta.score);
    
    // Try with JSON generating helper method
    
    exampleProductMetaJson =
        ProductFeedParserFlowlet.generateProductMetaJson(productMeta);
    processedJsonString =
        ProductFeedParserFlowlet.preProcessSocialActionJSON(
            exampleProductMetaJson);
    ProductMeta productMeta2 =
        gson.fromJson(processedJsonString, ProductMeta.class);

    assertEquals(productMeta.product_id, productMeta2.product_id);
    assertEquals(productMeta.store_id, productMeta2.store_id);
    assertEquals(productMeta.date, productMeta2.date);
    assertEquals(productMeta.category, productMeta2.category);
    assertEquals(productMeta.name, productMeta2.name);
    assertEquals(productMeta.score, productMeta2.score);
    
    // Try to serialize/deserialize in a tuple
    TupleSerializer serializer = new TupleSerializer(false);
    serializer.register(ProductMeta.class);
    Tuple sTuple = new TupleBuilder().set("meta", productMeta).create();
    byte [] bytes = serializer.serialize(sTuple);
    assertNotNull(bytes);
    Tuple dTuple = serializer.deserialize(bytes);
    ProductMeta productMeta3 = dTuple.get("meta");
    assertNotNull(productMeta);

    assertEquals(productMeta.product_id, productMeta3.product_id);
    assertEquals(productMeta.store_id, productMeta3.store_id);
    assertEquals(productMeta.date, productMeta3.date);
    assertEquals(productMeta.category, productMeta3.category);
    assertEquals(productMeta.name, productMeta3.name);
    assertEquals(productMeta.score, productMeta3.score);
    
    
    
  }
}
