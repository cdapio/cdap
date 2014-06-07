===============================================================
Building An Application Using MapReduce Jobs |br| and Workflows
===============================================================

.. reST Editor: .. section-numbering::
.. reST Editor: .. contents::

.. Slide Presentation HTML Generation
.. landslide: theme ../_theme/slides-generation/
.. landslide: build ../../html/

.. include:: ../_slide-fragments/continuuity_logo_copyright.rst

.. |br| raw:: html

   <br />

.. |br2| raw:: html

   <br /><br />

----

Exercise Objectives
====================

In this exercise, you will:

- Add a MapReduce job and Workflow to the example project
- Run the MapReduce job
- Run the Workflow
- View operations and results in Reactor Dashboard

----

Exercise Steps
========================

- Add imports
- Modify ``pom.xml``
- Modify ``ApplicationSpecification``
- Define MapReduce job
- Define ``beforeSubmit``
- Define ``Mapper``
- Define ``Reducer``
- Build, deploy, run and test



----

Add Imports
========================

Add these imports::

	import com.continuuity.api.mapreduce.AbstractMapReduce;
	import com.continuuity.api.mapreduce.MapReduceContext;
	import com.continuuity.api.mapreduce.MapReduceSpecification;
	import com.continuuity.api.schedule.Schedule;
	import com.continuuity.api.workflow.Workflow;
	import com.continuuity.api.workflow.WorkflowSpecification;


----

Modify ``pom.xml``
========================

Add this dependency::

	<dependency>
	  <groupId>org.apache.hadoop</groupId>
	  <artifactId>hadoop-common</artifactId>
	  <version>${hadoop.version}</version>
	  <scope>provided</scope>
	  <exclusions>
	    <exclusion>
	      <groupId>io.netty</groupId>
	      <artifactId>netty</artifactId>
	    </exclusion>
	    <exclusion>
	      <groupId>com.sun.jersey</groupId>
	      <artifactId>jersey-server</artifactId>
	    </exclusion>
	    <exclusion>
	      <groupId>org.jboss.netty</groupId>
	      <artifactId>netty</artifactId>
	    </exclusion>
	  </exclusions>
	</dependency>

----

Modify ``ApplicationSpecification``
===================================

Replace ``.noMapReduce()`` with::

	.withMapReduce()
	  .add(new SentimentAnalysisMapReduce())

Replace ``.noWorkflow()`` with::

	.withWorkflows()
	  .add(new SentimentAnalysisWorkflow())

----

Add MapReduce Job
========================

::

	public static class SentimentAnalysisMapReduce extends AbstractMapReduce {
	  
	  // Annotation indicates the DataSets used in this MapReduce
	  @UseDataSet("text-sentiments")
	  private SimpleTimeseriesTable textSentiments;
	  
	  @UseDataSet("sentiments")
	  private Table sentiments;
	
	  @Override
	  public MapReduceSpecification configure() {
	    return MapReduceSpecification.Builder.with()
	    .setName("SentimentCountMapReduce")
	    .setDescription("Sentiment count MapReduce job")
	    // Specify the DataSet for Mapper to read.
	    .useInputDataSet("text-sentiments")
	    // Specify the DataSet for Reducer to write.
	    .useOutputDataSet("sentiments")
	    .setMapperMemoryMB(512)
	    .setReducerMemoryMB(1024)
	    .build();
	  }
    

----

Define the ``beforeSubmit``
===========================

::

	@Override
	public void beforeSubmit(MapReduceContext context) throws Exception {
	  Job job = context.getHadoopJob();
	
	  // A Mapper processes sentiments from the stored sentences
	  context.setInput(textSentiments, textSentiments.getSplits());
	
	  job.setMapperClass(SentimentMapper.class);

	  // Set the output key of the Reducer class
	  job.setMapOutputKeyClass(Text.class);

	  // Set the output value of the Reducer class
	  job.setMapOutputValueClass(IntWritable.class);

	  job.setReducerClass(SentimentReducer.class);
	}
    

----

Define ``Mapper``
========================

::

	/**
	 * A Mapper that reads the sentiments from the text-sentiments
	 * DataSet and creates key value pairs, where the key is the
	 * sentiment and value is the occurence of a sentence. The Mapper
	 * receives a key value pair (<byte[], TimeseriesTable.Entry>)
	 * from the input DataSet and outputs data in another key value
	 * pair (<Text, IntWritable>) to the Reducer.
	 */
	public static class SentimentMapper extends Mapper<byte[], TimeseriesTable.Entry, Text,
	     IntWritable> {
	  // The output value
	  private static final IntWritable ONE = new IntWritable(1);
	  
	  @Override
	  public void map(byte[] key, TimeseriesTable.Entry entry, Context context)
	  throws IOException, InterruptedException {
	    // Send the key value pair to Reducer.
	    String sentiment = Bytes.toString(key);
	    context.write(new Text(sentiment), ONE);
	  }
	}
    

----

Define ``Reducer``
========================

::

	  /**
	   * Aggregate the number of sentences per sentiment and store the results in a Table.
	   */
	  public static class SentimentReducer extends Reducer<Text, IntWritable, byte[],
	      IntWritable> {
	    public void reduce(Text sentiment, Iterable<IntWritable> values, Context context)
	    throws IOException, InterruptedException {
	      int count = 0;
	      // Get the count of sentences
	      for (IntWritable val : values) {
	        count += val.get();
	      }
	      // Store aggregated results in output DataSet.
	      // Each sentiment's aggregated result is stored in a different row.
	      // Each result, the number of sentences, is an entry of the row.
	      // context.write(ROW_KEY, new TimeseriesTable.Entry(ROW_KEY, Bytes.toBytes(count),
	          key.get()));
	      context.write(Bytes.toBytes(sentiment.toString()), new IntWritable(count));
	    }
	  }

	} // Closes class SentimentAnalysisMapReduce


----

Define ``SentimentAnalysisWorkflow``
====================================

::

	/**
	 * Implements a simple Workflow with one Workflow action to run 
	 * the SentimentAnalysisMapReduce MapReduce job with a schedule
	 * that runs every day at 11:00 A.M.
	 */
	public class SentimentAnalysisWorkflow implements Workflow {
	  
	  @Override
	  public WorkflowSpecification configure() {
	    return WorkflowSpecification.Builder.with()
	    .setName("SentimentAnalysisWorkflow")
	    .setDescription("SentimentAnalysisWorkflow description")
	    .onlyWith(new SentimentAnalysisMapReduce())
	    .addSchedule(new Schedule("DailySchedule", "Run every day at 11:00 A.M.", "0 11 * * *",
	                              Schedule.Action.START))
	    .build();
	  }
	}

----

Build, Deploy and Test
======================

- Build using ``mvn clean package``
- Deploy the jar to Reactor after stopping any existing Flows
- Run the MapReduce job by 


----

Exercise Summary
===================

You should now be able to:

- Add MapReduce jobs and Workflows to a project
- Run MapReduce jobs
- Run Workflows
- View operations and results in the Reactor Dashboard

----

Exercise Completed
==================

`Chapter Index <return.html#e07>`__

