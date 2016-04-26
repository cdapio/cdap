/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark

import java.io.File
import java.net.URI
import java.util
import java.util.concurrent.{CountDownLatch, TimeUnit}

import co.cask.cdap.api._
import co.cask.cdap.api.app.ApplicationSpecification
import co.cask.cdap.api.data.batch.{BatchWritable, DatasetOutputCommitter, OutputFormatProvider, Split}
import co.cask.cdap.api.data.format.FormatSpecification
import co.cask.cdap.api.dataset.Dataset
import co.cask.cdap.api.flow.flowlet.StreamEvent
import co.cask.cdap.api.metrics.Metrics
import co.cask.cdap.api.plugin.PluginContext
import co.cask.cdap.api.spark.{SparkExecutionContext, SparkSpecification}
import co.cask.cdap.api.stream.GenericStreamEventData
import co.cask.cdap.api.workflow.{WorkflowInfo, WorkflowToken}
import co.cask.cdap.common.conf.ConfigurationUtil
import co.cask.cdap.data.stream.{StreamInputFormat, StreamUtils}
import co.cask.cdap.data2.metadata.lineage.AccessType
import co.cask.cdap.internal.app.runtime.DefaultTaskLocalizationContext
import co.cask.cdap.proto.Id
import co.cask.cdap.proto.id.StreamId
import co.cask.tephra.TransactionAware
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.executor.{DataWriteMethod, OutputMetrics}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler._
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.twill.api.RunId
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
  * Default implementation of [[co.cask.cdap.api.spark.SparkExecutionContext]].
  *
  * @param runtimeContext provides access to CDAP internal services
  * @param localizeResources a Map from name to local file that the user requested to localize during the
  *                          beforeSubmit call.
  */
class DefaultSparkExecutionContext(runtimeContext: SparkRuntimeContext,
                                   localizeResources: util.Map[String, File],
                                   hostname: String) extends SparkExecutionContext with AutoCloseable {
  // Import the companion object for static fields
  import DefaultSparkExecutionContext._

  private val taskLocalizationContext = new DefaultTaskLocalizationContext(localizeResources)
  private val transactional = new SparkTransactional(runtimeContext.getTransactionSystemClient,
                                                     runtimeContext.getDatasetCache)
  private val workflowInfo = Option(runtimeContext.getWorkflowInfo)
  private val sparkTxService = new SparkTransactionService(runtimeContext.getTransactionSystemClient, hostname)
  private val applicationEndLatch = new CountDownLatch(1)

  // Start the Spark TX service
  sparkTxService.startAndWait()

  // Attach a listener to the SparkContextCache, which will in turn listening to events from SparkContext.
  SparkRuntimeEnv.addSparkListener(new SparkListener {

    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) = applicationEndLatch.countDown

    override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
      val jobId = Integer.valueOf(jobStart.jobId)
      val stageIds = jobStart.stageInfos.map(info => info.stageId: Integer).toSet
      val sparkTransaction = Option(jobStart.properties.getProperty(SparkTransactional.ACTIVE_TRANSACTION_KEY))
          .flatMap(key => if (key.isEmpty) None else Option(transactional.getTransactionInfo(key)))

      sparkTransaction.fold({
        LOG.debug("Spark program={}, runId={}, jobId={} starts without transaction",
                  runtimeContext.getProgram.getId.toEntityId, getRunId, jobId)
        sparkTxService.jobStarted(jobId, stageIds)
      })(info => {
        LOG.debug("Spark program={}, runId={}, jobId={} starts with auto-commit={} on transaction {}",
                  runtimeContext.getProgram.getId.toEntityId, getRunId, jobId,
                  info.commitOnJobEnded().toString, info.getTransaction)
        sparkTxService.jobStarted(jobId, stageIds, info)
        info.onJobStarted()
      })
    }

    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
      sparkTxService.jobEnded(jobEnd.jobId, jobEnd.jobResult == JobSucceeded)
    }
  })

  override def close() = {
    try {
      // If there is a SparkContext, wait for the ApplicationEnd callback
      // This make sure all jobs' transactions are committed/invalidated
      SparkRuntimeEnv.stop().foreach(sc => applicationEndLatch.await())
    } finally {
      sparkTxService.stopAndWait()
    }
  }

  override def getApplicationSpecification: ApplicationSpecification = runtimeContext.getApplicationSpecification

  override def getRuntimeArguments: util.Map[String, String] = runtimeContext.getRuntimeArguments

  override def getRunId: RunId = runtimeContext.getRunId

  override def getNamespace: String = runtimeContext.getProgram.getId.getNamespaceId

  override def getAdmin: Admin = runtimeContext.getAdmin

  override def getSpecification: SparkSpecification = runtimeContext.getSparkSpecification

  override def getLogicalStartTime: Long = runtimeContext.getLogicalStartTime

  override def getServiceDiscoverer: ServiceDiscoverer = new SparkServiceDiscoverer(runtimeContext)

  override def getMetrics: Metrics = new SparkUserMetrics(runtimeContext)

  override def getPluginContext: PluginContext = new SparkPluginContext(runtimeContext)

  override def getWorkflowToken: Option[WorkflowToken] = workflowInfo.map(_.getWorkflowToken)

  override def getWorkflowInfo: Option[WorkflowInfo] = workflowInfo

  override def getLocalizationContext: TaskLocalizationContext = taskLocalizationContext

  override def execute(runnable: TxRunnable): Unit = {
    transactional.execute(runnable)
  }

  override def fromDataset[K: ClassTag, V: ClassTag](sc: SparkContext,
                                                     datasetName: String,
                                                     arguments: Map[String, String],
                                                     splits: Option[Iterable[_ <: Split]]): RDD[(K, V)] = {
    new DatasetRDD[K, V](sc, createDatasetCompute, runtimeContext.getConfiguration, datasetName, arguments, splits,
                         getTxServiceBaseURI(sc, sparkTxService.getBaseURI))
  }

  override def fromStream[T: ClassTag](sc: SparkContext, streamName: String, startTime: Long, endTime: Long)
                                      (implicit decoder: StreamEvent => T): RDD[T] = {
    val rdd: RDD[(Long, StreamEvent)] = fromStream(sc, streamName, startTime, endTime, None)

    // Wrap the StreamEvent with a SerializableStreamEvent
    // Don't use rdd.values() as it brings in implicit object from SparkContext, which is not available in Spark 1.2
    rdd.map(t => new SerializableStreamEvent(t._2)).map(decoder)
  }

  override def fromStream[T: ClassTag](sc: SparkContext, streamName: String, formatSpec: FormatSpecification,
                                       startTime: Long, endTime: Long): RDD[(Long, GenericStreamEventData[T])] = {
    fromStream(sc, streamName, startTime, endTime, Some(formatSpec))
  }

  /**
    * Creates a [[org.apache.spark.rdd.RDD]] by reading from the given stream and time range.
    *
    * @param sc the [[org.apache.spark.SparkContext]] to use
    * @param streamName name of the stream
    * @param startTime  the starting time of the stream to be read in milliseconds (inclusive);
    *                   passing in `0` means start reading from the first event available in the stream.
    * @param endTime the ending time of the streams to be read in milliseconds (exclusive);
    *                passing in `Long#MAX_VALUE` means read up to latest event available in the stream.
    * @param formatSpec if provided, it describes the format in the stream and will be used to decode stream events
    *                   to the given value type `T`
    * @return a new [[org.apache.spark.rdd.RDD]] instance that reads from the given stream.
    */
  private def fromStream[T: ClassTag](sc: SparkContext, streamName: String,
                                      startTime: Long, endTime: Long,
                                      formatSpec: Option[FormatSpecification]): RDD[(Long, T)] = {
    val streamId = new StreamId(getNamespace, streamName)

    // Clone the configuration since it's dataset specification and shouldn't affect the global hConf
    val configuration = configureStreamInput(new Configuration(runtimeContext.getConfiguration),
                                             streamId, startTime, endTime, formatSpec)

    val valueClass = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    val rdd = sc.newAPIHadoopRDD(configuration, classOf[StreamInputFormat[LongWritable, T]],
                                 classOf[LongWritable], valueClass)
    recordStreamUsage(streamId)
    rdd.map(t => (t._1.get(), t._2))
  }

  override def saveAsDataset[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)], datasetName: String,
                                                       arguments: Map[String, String]): Unit = {
    transactional.executeWithActiveOrLongTx(new SparkTxRunnable {
      override def run(context: SparkDatasetContext) = {
        val sc = rdd.sparkContext
        val dataset: Dataset = context.getDataset(datasetName, arguments, AccessType.WRITE)
        val outputCommitter = dataset match {
          case outputCommitter: DatasetOutputCommitter => Some(outputCommitter)
          case _ => None
        }

        try {
          dataset match {
            case outputFormatProvider: OutputFormatProvider =>
              val conf = new Configuration(runtimeContext.getConfiguration)

              ConfigurationUtil.setAll(outputFormatProvider.getOutputFormatConfiguration, conf)
              conf.set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, outputFormatProvider.getOutputFormatClassName)

              // In Spark 1.2, we have to use the SparkContext.rddToPairRDDFunctions because the implicit
              // conversion from RDD is not available.
              if (sc.version == "1.2" || sc.version.startsWith("1.2.")) {
                SparkContext.rddToPairRDDFunctions(rdd).saveAsNewAPIHadoopDataset(conf)
              } else {
                rdd.saveAsNewAPIHadoopDataset(conf)
              }

            case batchWritable: BatchWritable[K, V] =>
              val txServiceBaseURI = getTxServiceBaseURI(sc, sparkTxService.getBaseURI)
              sc.runJob(rdd, createBatchWritableFunc(datasetName, arguments, txServiceBaseURI))

            case _ =>
              throw new IllegalArgumentException("Dataset is neither a OutputFormatProvider nor a BatchWritable")
          }
          outputCommitter.foreach(_.onSuccess())
        } catch {
          case t: Throwable =>
            outputCommitter.foreach(_.onFailure())
            throw t
        }
      }
    }, false)
  }

  private def configureStreamInput(configuration: Configuration, streamId: StreamId, startTime: Long,
                                   endTime: Long, formatSpec: Option[FormatSpecification]): Configuration = {
    val streamConfig = runtimeContext.getStreamAdmin.getConfig(streamId.toId)
    val streamPath = StreamUtils.createGenerationLocation(streamConfig.getLocation,
                                                          StreamUtils.getGeneration(streamConfig))

    StreamInputFormat.setTTL(configuration, streamConfig.getTTL)
    StreamInputFormat.setStreamPath(configuration, streamPath.toURI)
    StreamInputFormat.setTimeRange(configuration, startTime, endTime)
    // Either use the identity decoder or use the format spec to decode
    formatSpec.fold(
      StreamInputFormat.inferDecoderClass(configuration, classOf[StreamEvent])
    )(
      spec => StreamInputFormat.setBodyFormatSpecification(configuration, spec)
    )
    configuration
  }

  private def recordStreamUsage(streamId: StreamId): Unit = {
    val oldStreamId = streamId.toId

    // Register for stream usage for the Spark program
    val oldProgramId = runtimeContext.getProgram.getId
    val owners = List(oldProgramId)
    try {
      runtimeContext.getStreamAdmin.register(owners, oldStreamId)
      runtimeContext.getStreamAdmin.addAccess(new Id.Run(oldProgramId, getRunId.getId), oldStreamId, AccessType.READ)
    }
    catch {
      case e: Exception =>
        LOG.warn("Failed to register usage of {} -> {}", streamId, owners, e)
    }
  }

  /**
    * Creates a [[co.cask.cdap.app.runtime.spark.DatasetCompute]] for the DatasetRDD to use.
    */
  private def createDatasetCompute: DatasetCompute = {
    new DatasetCompute {
      override def apply[T: ClassTag](datasetName: String, arguments: Map[String, String], f: (Dataset) => T): T = {
        val result = new Array[T](1)

        // For RDD to compute partitions, a transaction is needed in order to gain access to dataset instance.
        // It should either be using the active transaction (explicit transaction), or create a new transaction
        // but leave it open so that it will be used for all stages in same job execution and get committed when
        // the job ended.
        transactional.executeWithActiveOrLongTx(new SparkTxRunnable {
          override def run(context: SparkDatasetContext) = {
            val dataset: Dataset = context.getDataset(datasetName, arguments, AccessType.READ)
            try {
              result(0) = f(dataset)
            } finally {
              context.releaseDataset(dataset)
            }
          }
        }, true)
        result(0)
      }
    }
  }
}

/**
  * Companion object for holding static fields and methods.
  */
object DefaultSparkExecutionContext {

  private val LOG = LoggerFactory.getLogger(classOf[DefaultSparkExecutionContext])
  private var txServiceBaseURI: Option[Broadcast[URI]] = None

  /**
    * Creates a [[org.apache.spark.broadcast.Broadcast]] for the base URI
    * of the [[co.cask.cdap.app.runtime.spark.SparkTransactionService]]
    */
  private def getTxServiceBaseURI(sc: SparkContext, baseURI: URI): Broadcast[URI] = {
    this.synchronized {
      this.txServiceBaseURI match {
        case Some(uri) => uri
        case None =>
          val broadcast = sc.broadcast(baseURI)
          txServiceBaseURI = Some(broadcast)
          broadcast
      }
    }
  }

  /**
    * Creates a function used by [[org.apache.spark.SparkContext]] runJob for writing data to a
    * Dataset that implements [[co.cask.cdap.api.data.batch.BatchWritable]]. The returned function
    * will be executed in executor nodes.
    */
  private def createBatchWritableFunc[K, V]
      (datasetName: String,
       arguments: Map[String, String],
       txServiceBaseURI: Broadcast[URI]) = (context: TaskContext, itor: Iterator[(K, V)]) => {

    val outputMetrics = new BatchWritableMetrics
    context.taskMetrics.outputMetrics = Option(outputMetrics)

    val sparkTxClient = new SparkTransactionClient(txServiceBaseURI.value)
    val datasetCache = SparkRuntimeContextProvider.get().getDatasetCache
    val dataset: Dataset = datasetCache.getDataset(datasetName, arguments, true, AccessType.WRITE)

    try {
      // Creates an Option[TransactionAware] if the dataset is a TransactionAware
      val txAware = dataset match {
        case txAware: TransactionAware => Some(txAware)
        case _ => None
      }

      // Try to get the transaction for this stage. Hardcoded the timeout to 10 seconds for now
      txAware.foreach(_.startTx(sparkTxClient.getTransaction(context.stageId(), 10, TimeUnit.SECONDS)))

      // Write through BatchWritable.
      val writable = dataset.asInstanceOf[BatchWritable[K, V]]
      var records = 0
      while (itor.hasNext) {
        val pair = itor.next()
        writable.write(pair._1, pair._2)
        outputMetrics.incrementRecordWrite(1)

        // Periodically calling commitTx to flush changes. Hardcoded to 1000 records for now
        if (records > 1000) {
          txAware.foreach(_.commitTx())
          records = 0
        }
        records += 1
      }

      // Flush all writes
      txAware.foreach(_.commitTx())
    } finally {
      dataset.close()
    }
  }

  /**
    * Implementation of [[org.apache.spark.executor.OutputMetrics]] for recording metrics output from
    * [[co.cask.cdap.api.data.batch.BatchWritable]].
    */
  private class BatchWritableMetrics extends OutputMetrics(DataWriteMethod.Hadoop) {
    private var records = 0

    def incrementRecordWrite(records: Int) = this.records += records

    override def recordsWritten = records
  }
}
