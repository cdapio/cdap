/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

/**
 * <h1>WIP</h1>
 * <h1>Application</h1>
 * Application is a logical grouping of Streams, Datasets, Flows & Procedures that is deployable.
 *
 * <p>
 *   In order to create and application in AppFabric, you begin by implementing an interface {@link Application}.
 *   Within the implementation of {@link Application.configure();} you create an {@link ApplicationSpecification}
 *   that defines and/or create all the components of an {@link Application}.
 *   To create an application, one need to implement the interface {@code Application}. Within the
 *   Application's configure you will create all the different entities that are needed to form an
 *   application.
 * </p>
 *
 * <p>
 * Example usage
 *   <pre>
 *   public MyApplication implements Application {
 *
 *     public ApplicationSpecification configure() {
 *
 *       return ApplicationSpecification.Builder.with()
 *             .setName("myFirstApp")
 *             .setDescription("This is my first application")
 *             .withStreams().add(new Stream("text"))
 *                           .add(new Stream("log"))
 *             .withDataSets().add(new KeyValueTable("mytable"))
 *                            .add(new SimpleTimeseriesTable("tstable"))
 *             .withFlows().add(new MyFirstFlow())
 *                         .add(new LogProcessFlow())
 *             .withProcedures().add(new KeyValueLookupProcedure())
 *                              .add(new LogProcedure())
 *             .build();
 *     }
 *   }
 *   </pre>
 * </p>
 *
 * <h1>Flow</h1>
 * A {@link Flow} is type of Processor that enables real time processing of events as a DAG.
 * A {@link com.continuuity.api.flow.Flow} is set of {@link com.continuuity.api.flow.flowlet.Flowlet}
 * connected by queues.
 * <p>
 *   In order to define a {@link com.continuuity.api.flow.Flow}, you need to extends
 *   from {@link com.continuuity.api.flow.Flow} interface.
 * </p>
 * <h2>Flowlet</h2>
 * {@link com.continuuity.api.flow.flowlet.Flowlet} is a indivisble unit of a {@link com.continuuity.api.flow.Flow}
 * that defines a business logic for processing events received on input and also can emit new events on the output
 * for downstream processing. There are two types of {@link com.continuuity.api.flow.flowlet.Flowlet}.
 * <ul>
 *   <li>Generic Flowlet</li>
 *   <li>Typed Flowlet</li>
 * </ul>
 * <h3>Generic Flowlet</h3>
 * <p>
 *   Generic {@link com.continuuity.api.flow.flowlet.Flowlet} allows you define a event of type <code>Tuple</code>.
 *   A <code>Tuple</code> defines a generic type that can consists of have any type of objects within in it.
 *
 *   Example of flowlet that is generic:
 *   <p>
 *     <pre>
 *       {@code
 *       public class SentimentAnalyzer extends AbstractFlowlet {
 *
 *          FlowletSpecification configure() {
 *            return FlowletSpecification.Builder.with()
 *                      .set(...)
 *          }
 *
 *          public void process(Tuple tuple) {
 *            ....
 *          }
 *       }
 *       }
 *     </pre>
 *   </p>
 * </p>
 * <h3>Typed Flowlet</h3>
 *   Typed {@link com.continuuity.api.flow.flowlet.Flowlet}
 * <h1>Procedure</h1>
 *
 * <h1>Dataset</h1>
 *
 * <h1>Stream</h1>
 *
 * <h1>Example</h1>
 *
 * Following is simple example of an application that reads an event from a {@link com.continuuity.api.data.stream.Stream}
 * extracts from event header, tokenizes it and counts number of tokens. Also, this application has a way to retrieve
 * the token counts for a given token. So, this application comprises of a Stream, Flow, Dataset and a Procedure.
 *
 * <p>
 *   <code>MyRecord</code> defines the structure that application captures the event into.
 *   <pre>
 *       public static final class MyRecord {
 *          private String title;
 *          private String text;
 *
 *          public MyRecord setTitle(String title) {
 *            this.title = title;
 *            return this;
 *          }
 *
 *          public MyRecord setText(String text) {
 *            this.text = text;
 *            return this;
 *          }
 *
 *          public String getTitle() {
 *            return title;
 *          }
 *
 *          public String getText() {
 *            return text;
 *          }
 *      }
 *   </pre>
 * </p>
 *
 * <p>
 *   Defining a <code>WordCountApp</code>
 *   <pre>
 *     public class WordCountApp implements Application {
 *       {@literal @Override}
 *       public ApplicationSpecification configure() {
 *         return ApplicationSpecification.Builder.with()
 *            .setName("WordCountApp")
 *            .setDescription("Application for counting words")
 *            .withStreams().add(new Stream("text")
 *            .withDataSets().add(new KeyValueTable("mydataset"))
 *            .withFlows().add(new WordCountFlow())
 *            .noProcedure().build();
 *       }
 *     }
 *   </pre>
 * </p>
 *
 * <p>
 *   <code>WordCountFlow</code> reads an event from {@link Stream} <code>text</code>, tokenizes it and aggregates
 *   the counts for each token generated.
 *   <pre>
 *     public class WordCountFlow implements Flow {
 *       public FlowSpecification configure() {
 *         return FlowSpecification.Builder.with()
 *             .setName("WordCountFlow")
 *             .setDescription("Flow for counting tokens")
 *             .withFlowlets()
 *                .add(new StreamReader()).apply()
 *                .add(new Tokenizer()).apply()
 *                .add(new TokenCounter()).apply()
 *             .connect()
 *                .from(new Stream("text")).to(new StreamReader())
 *                .from(new StreamReader()).to(new Tokenizer())
 *                .from(new Tokenizer()).to(new TokenCounter())
 *             .build();
 *       }
 *     }
 *   </pre>
 * </p>
 *
 * <p>
 *  Following are different {@link com.continuuity.api.flow.flowlet.Flowlet} that make up the <code>WordCountFlow</code>
 * </p>
 *
 * <p>
 *   <code>StreamReader</code> {@link com.continuuity.api.flow.flowlet.Flowlet} is flowlet that reads in an event
 *   from the {@link Stream} of name <code>text</code>. After reading an event it emits <code>MyRecord</code> object
 *   as defined by the emitter <code>OutputEmitter&lt;MyRecord&gt; output</code>.
 *   <pre>
 *     public class StreamReader extends AbstractFlowlet {
 *       private OutputEmitter&lt;MyRecord&gt; output;
 *
 *       public void process(StreamEvent event) throws CharacterCodingException {
 *         ByteBuffer buf = event.getBody();
 *         output.emit(new MyRecord()
 *                       .setTitle(event.getHeaders().get("title")
 *                       .setText(buf == null ? null : Charset.forName("UTF-8").newDecoder().decode(buf).toString());
 *       }
 *     }
 *   </pre>
 * </p>
 *
 * <p>
 *   <code>Tokenizer</code> reads an event from <code>StreamReader</code> as <code>MyRecord</code> and parses the
 *   <code>text</code> and <code>title</code> into tokens. <code>Tokenizer</code>
 *   {@link com.continuuity.api.flow.flowlet.Flowlet} is strongly typed as it expected the input to be of type
 *   <code>MyRecord</code>
 *   <pre>
 *     public class Tokenizer extends AbstractFlowlet {
 *       {@literal @Output}
 *       private OutputEmitter<Map<String, String>> output;
 *
 *       {@literal @Process}
 *       public void tokenizer(MyRecord data) {
 *         tokenize(data.getTitle(), "title");
 *         tokenize(data.getText(), "text");
 *       }
 *
 *       private void tokenize(String str, String field) {
 *         if(str == null) {
 *           return;
 *         }
 *         final String delimiters = "[ .-]";
 *         for (String token : str.split(delimiters)) {
 *           output.emit(ImmutableMap.of("field", field, "word", token));
 *         }
 *       }
 *     }
 *   </pre>
 * </p>
 *
 * <p>
 *   <pre>
 *     public class TokenCounter implements AbstractFlowlet {
 *       {@literal @DataSet("mydataset")}
 *       private KeyValueTable counters;
 *
 *       {@literal @Process}
 *       public void process(Map<String, String> fields) {
 *         String word = fields.get("word");
 *         if(token == null) {
 *          return;
 *         }
 *
 *         String field = fields.get("field");
 *         if(field != null) {
 *           word = field + ":" + word;
 *         }
 *         counters.stage(new KeyValueTable.IncrementKey(word.getBytes(Charset.forName("UTF-8"))));
 *       }
 *     }
 *   </pre>
 * </p>
 *
 * <h1>More examples</h1>
 * <h2>How to create an enrichment pipeline</h2>
 * <p>
 *   <h3>Team 1</h3>
 *   <p>Defines a type Document, let's say was extracted from crawling.</p>
 *   <pre>
 *      public interface Document {
 *        public Integer getDocumentId();
 *        public String  getDocumentBody();
 *        public String  getUrl();
 *      }
 *   </pre>
 *   <p>
 *     A flowlet managed by Team 1 will emit type Document, anyone connecting to
 *     this flowlet will be able to consume type Document or any type that extends
 *     from Document type.
 *   </p>
 *   <pre>
 *     public DocumentCrawler extends AbstractFlowlet {
 *       private OutputEmitter<Document> out;
 *
 *       @Process
 *       public void generate() {
 *         out.emit(new Document() { ... });
 *       }
 *     }
 *   </pre>
 * </p>
 *
 * <p>
 *   <h3>Team 2</h3>
 *   <p>Enriches the document, let's say it's adding page rank to document</p>
 *   <pre>
 *     public interface DocumentRank extends Document {
 *       public Float getPageRank();
 *     }
 *   </pre>
 *   <p>
 *     A Flowlet managed by Team 2 will emit DocumentRank. DocumentRank is a Document
 *     with Page Rank enrichment on Document.
 *   </p>
 *   <pre>
 *     public DocumentRanker extends AbstractFlowlet {
 *       private OutputEmitter<DocumentRank> out;
 *
 *       @Process
 *       public void process(DocumentRank ranker) {
 *          ranker.setPageRank(...);
 *          out.emit(ranker);
 *       }
 *     }
 *   </pre>
 * </p>
 *
 * <p>
 *   <h3>Team 3</h3>
 *   <p>Enriches the document, needs Page Rank to generate entities within the document </p>
 *   <pre>
 *     public interface DocumentEntities extends DocumentRank {
 *       public String[] getEntities();
 *     }
 *   </pre>
 * </p>
 *
 * <p>
 *   <h2>Team 4</h2>
 *   <p>Team 4 needs a consume a basic Document</p>
 * </p>
 *
 */
package com.continuuity.api;
