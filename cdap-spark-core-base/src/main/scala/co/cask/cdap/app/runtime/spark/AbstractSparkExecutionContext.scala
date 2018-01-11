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

import co.cask.cdap.api._
import co.cask.cdap.api.app.ApplicationSpecification
import co.cask.cdap.api.data.batch.BatchWritable
import co.cask.cdap.api.data.batch.DatasetOutputCommitter
import co.cask.cdap.api.data.batch.OutputFormatProvider
import co.cask.cdap.api.data.batch.Split
import co.cask.cdap.api.data.format.FormatSpecification
import co.cask.cdap.api.dataset.Dataset
import co.cask.cdap.api.flow.flowlet.StreamEvent
import co.cask.cdap.api.messaging.MessagingContext
import co.cask.cdap.api.metrics.Metrics
import co.cask.cdap.api.plugin.PluginContext
import co.cask.cdap.api.preview.DataTracer
import co.cask.cdap.api.security.store.SecureStore
import co.cask.cdap.api.spark.SparkExecutionContext
import co.cask.cdap.api.spark.SparkSpecification
import co.cask.cdap.api.spark.dynamic.SparkInterpreter
import co.cask.cdap.api.stream.GenericStreamEventData
import co.cask.cdap.api.workflow.WorkflowInfo
import co.cask.cdap.api.workflow.WorkflowToken
import co.cask.cdap.app.runtime.spark.SparkTransactional.TransactionType
import co.cask.cdap.app.runtime.spark.data.DatasetRDD
import co.cask.cdap.app.runtime.spark.dynamic.AbstractSparkCompiler
import co.cask.cdap.app.runtime.spark.dynamic.SparkClassFileHandler
import co.cask.cdap.app.runtime.spark.dynamic.SparkCompilerCleanupManager
import co.cask.cdap.app.runtime.spark.dynamic.URLAdder
import co.cask.cdap.app.runtime.spark.preview.SparkDataTracer
import co.cask.cdap.app.runtime.spark.stream.SparkStreamInputFormat
import co.cask.cdap.common.conf.ConfigurationUtil
import co.cask.cdap.common.conf.Constants
import co.cask.cdap.common.utils.DirUtils
import co.cask.cdap.data.LineageDatasetContext
import co.cask.cdap.data.stream.AbstractStreamInputFormat
import co.cask.cdap.data.stream.StreamUtils
import co.cask.cdap.data2.metadata.lineage.AccessType
import co.cask.cdap.internal.app.runtime.DefaultTaskLocalizationContext
import co.cask.cdap.proto.id.StreamId
import co.cask.cdap.proto.security.Action
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler._
import org.apache.tephra.TransactionAware
import org.apache.twill.api.RunId
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.File
import java.io.IOException
import java.net.URI
import java.net.URL
import java.util
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

import co.cask.cdap.api.schedule.TriggeringScheduleInfo

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.tools.nsc.Settings

/**
  * Default implementation of [[co.cask.cdap.api.spark.SparkExecutionContext]].
  *
  * @param runtimeContext provides access to CDAP internal services
  * @param localizeResources a Map from name to local file that the user requested to localize during the
  *                          beforeSubmit call.
  */
abstract class AbstractSparkExecutionContext(sparkClassLoader: SparkClassLoader,
                                             localizeResources: util.Map[String, File])
  extends SparkExecutionContext with AutoCloseable {

  // Import the companion object for static fields
  import AbstractSparkExecutionContext._

  protected val runtimeContext = sparkClassLoader.getRuntimeContext

  private val taskLocalizationContext = new DefaultTaskLocalizationContext(localizeResources)
  private val transactional = new SparkTransactional(runtimeContext.getTransactionSystemClient,
                                                     runtimeContext.getDatasetCache,
                                                     runtimeContext.getRetryStrategy)
  private val workflowInfo = Option(runtimeContext.getWorkflowInfo)
  private val sparkTxHandler = new SparkTransactionHandler(runtimeContext.getTransactionSystemClient)
  private val sparkClassFileHandler = new SparkClassFileHandler
  private val sparkDriveHttpService = new SparkDriverHttpService(runtimeContext.getProgramName,
                                                                 runtimeContext.getHostname,
                                                                 sparkTxHandler, sparkClassFileHandler)
  private val applicationEndLatch = new CountDownLatch(1)
  private val authorizationEnforcer = runtimeContext.getAuthorizationEnforcer
  private val authenticationContext = runtimeContext.getAuthenticationContext
  private val compilerCleanupManager = new SparkCompilerCleanupManager
  private val interpreterCount = new AtomicInteger(0)

  // Start the Spark driver http service
  sparkDriveHttpService.startAndWait()

  // Set the spark.repl.class.uri that points to the http service if spark-repl is present
  try {
    sparkClassLoader.loadClass(SparkRuntimeContextProvider.EXECUTOR_CLASSLOADER_NAME)
    SparkRuntimeEnv.setProperty("spark.repl.class.uri", sparkDriveHttpService.getBaseURI.toString)
  } catch {
    case _: ClassNotFoundException => // no-op
  }

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
                  runtimeContext.getProgram.getId, getRunId, jobId)
        sparkTxHandler.jobStarted(jobId, stageIds)
      })(info => {
        LOG.debug("Spark program={}, runId={}, jobId={} starts with auto-commit={} on transaction {}",
                  runtimeContext.getProgram.getId, getRunId, jobId,
                  info.commitOnJobEnded().toString, info.getTransaction)
        sparkTxHandler.jobStarted(jobId, stageIds, info)
        info.onJobStarted()
      })
    }

    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
      sparkTxHandler.jobEnded(jobEnd.jobId, jobEnd.jobResult == JobSucceeded)
    }
  })

  override def close() = {
    try {
      // If there is a SparkContext, wait for the ApplicationEnd callback
      // This make sure all jobs' transactions are committed/invalidated
      SparkRuntimeEnv.stop().foreach(sc => applicationEndLatch.await())
    } finally {
      try {
        sparkDriveHttpService.stopAndWait()
      } finally {
        compilerCleanupManager.close()
      }
    }
  }

  override def getApplicationSpecification: ApplicationSpecification = runtimeContext.getApplicationSpecification

  override def getClusterName: String = runtimeContext.getClusterName

  override def getRuntimeArguments: util.Map[String, String] = runtimeContext.getRuntimeArguments

  override def getRunId: RunId = runtimeContext.getRunId

  override def getNamespace: String = runtimeContext.getProgram.getNamespaceId

  override def getAdmin: Admin = runtimeContext.getAdmin

  override def getSpecification: SparkSpecification = runtimeContext.getSparkSpecification

  override def getLogicalStartTime: Long = runtimeContext.getLogicalStartTime

  override def getServiceDiscoverer: ServiceDiscoverer = new SparkServiceDiscoverer(runtimeContext)

  override def getMetrics: Metrics = new SparkUserMetrics(runtimeContext)

  override def getSecureStore: SecureStore = new SparkSecureStore(runtimeContext)

  // TODO: CDAP-7807. Returns one that is serializable
  override def getMessagingContext: MessagingContext = runtimeContext

  override def getPluginContext: PluginContext = new SparkPluginContext(runtimeContext)

  override def getWorkflowToken: Option[WorkflowToken] = workflowInfo.map(_.getWorkflowToken)

  override def getWorkflowInfo: Option[WorkflowInfo] = workflowInfo

  override def getLocalizationContext: TaskLocalizationContext = taskLocalizationContext

  override def execute(runnable: TxRunnable): Unit = {
    transactional.execute(runnable)
  }

  override def execute(timeout: Int, runnable: TxRunnable): Unit = {
    transactional.execute(timeout, runnable)
  }

  override def createInterpreter(): SparkInterpreter = {
    // Create the class directory
    val cConf = runtimeContext.getCConfiguration
    val tempDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                           cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile
    val classDir = new File(tempDir,
 	runtimeContext.getProgramRunId.toIdParts.mkString(".") + "-classes-" + interpreterCount.incrementAndGet())
    if (!DirUtils.mkdirs(classDir)) {
      throw new IOException("Failed to create directory " + classDir + " for storing compiled class files.")
    }

    // Setup classpath and classloader
    val settings = AbstractSparkCompiler.setClassPath(new Settings())

    val urlAdded = new mutable.HashSet[URL]
    val urlAdder = new URLAdder {
      override def addURLs(urls: URL*) = {
        sparkClassFileHandler.addURLs(urls: _*)
        urlAdded ++= urls
      }
    }
    urlAdder.addURLs(classDir.toURI.toURL)

    // Creates the interpreter. The cleanup is just to remove it from the cleanup manager
    var interpreterRef: Option[SparkInterpreter] = None
    val interpreter = createInterpreter(settings, classDir, urlAdder, () => {
      interpreterRef.foreach(compilerCleanupManager.removeCompiler)
    })
    interpreterRef = Some(interpreter)

    // The closeable for removing the class file directory from the file handler as well as deleting it
    val closeable = new Closeable {
      val closed = new AtomicBoolean()
      override def close(): Unit = {
        if (closed.compareAndSet(false, true)) {
          sparkClassFileHandler.removeURLs(urlAdded)
          DirUtils.deleteDirectoryContents(classDir, true)
        }
      }
    }
    compilerCleanupManager.addCompiler(interpreter, closeable)
    interpreter
  }

  override def fromDataset[K: ClassTag, V: ClassTag](sc: SparkContext,
                                                     datasetName: String,
                                                     arguments: Map[String, String],
                                                     splits: Option[Iterable[_ <: Split]]): RDD[(K, V)] = {
    new DatasetRDD[K, V](sc, createDatasetCompute(), runtimeContext.getConfiguration, getNamespace, datasetName,
                         arguments, splits, getDriveHttpServiceBaseURI(sc))
  }

  override def fromDataset[K: ClassTag, V: ClassTag](sc: SparkContext,
                                                     namespace: String,
                                                     datasetName: String,
                                                     arguments: Map[String, String],
                                                     splits: Option[Iterable[_ <: Split]]): RDD[(K, V)] = {
    new DatasetRDD[K, V](sc, createDatasetCompute(), runtimeContext.getConfiguration, namespace,
                         datasetName, arguments, splits, getDriveHttpServiceBaseURI(sc))
  }

  override def fromStream[T: ClassTag](sc: SparkContext, streamName: String, startTime: Long, endTime: Long)
                                      (implicit decoder: StreamEvent => T): RDD[T] = {
    val rdd: RDD[(Long, StreamEvent)] = fromStream(sc, getNamespace, streamName, startTime, endTime, None)

    // Wrap the StreamEvent with a SerializableStreamEvent
    // Don't use rdd.values() as it brings in implicit object from SparkContext, which is not available in Spark 1.2
    rdd.map(t => new SerializableStreamEvent(t._2)).map(decoder)
  }

  override def fromStream[T: ClassTag](sc: SparkContext, namespace: String, streamName: String, startTime: Long,
                                       endTime: Long) (implicit decoder: StreamEvent => T): RDD[T] = {
    val rdd: RDD[(Long, StreamEvent)] = fromStream(sc, namespace, streamName, startTime, endTime, None)

    // Wrap the StreamEvent with a SerializableStreamEvent
    // Don't use rdd.values() as it brings in implicit object from SparkContext, which is not available in Spark 1.2
    rdd.map(t => new SerializableStreamEvent(t._2)).map(decoder)
  }

  override def fromStream[T: ClassTag](sc: SparkContext, streamName: String, formatSpec: FormatSpecification,
                                       startTime: Long, endTime: Long): RDD[(Long, GenericStreamEventData[T])] = {
    fromStream(sc, getNamespace, streamName, startTime, endTime, Some(formatSpec))
  }

  override def fromStream[T: ClassTag](sc: SparkContext, namespace: String,
                                       streamName: String,
                                       formatSpec: FormatSpecification,
                                       startTime: Long, endTime: Long): RDD[(Long, GenericStreamEventData[T])] = {
    fromStream(sc, namespace, streamName, startTime, endTime, Some(formatSpec))
  }

  /**
    * Creates a [[org.apache.spark.rdd.RDD]] by reading from the given stream and time range.
    *
    * @param sc the [[org.apache.spark.SparkContext]] to use
    * @param namespace namespace of the stream
    * @param streamName name of the stream
    * @param startTime  the starting time of the stream to be read in milliseconds (inclusive);
    *                   passing in `0` means start reading from the first event available in the stream.
    * @param endTime the ending time of the streams to be read in milliseconds (exclusive);
    *                passing in `Long#MAX_VALUE` means read up to latest event available in the stream.
    * @param formatSpec if provided, it describes the format in the stream and will be used to decode stream events
    *                   to the given value type `T`
    * @return a new [[org.apache.spark.rdd.RDD]] instance that reads from the given stream.
    */
  private def fromStream[T: ClassTag](sc: SparkContext, namespace: String, streamName: String,
                                      startTime: Long, endTime: Long,
                                      formatSpec: Option[FormatSpecification]): RDD[(Long, T)] = {
    val streamId = new StreamId(namespace, streamName)

    // Clone the configuration since it's dataset specification and shouldn't affect the global hConf
    val configuration = configureStreamInput(new Configuration(runtimeContext.getConfiguration),
      streamId, startTime, endTime, formatSpec)

    val valueClass = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    val rdd = sc.newAPIHadoopRDD(configuration, classOf[SparkStreamInputFormat[LongWritable, T]],
      classOf[LongWritable], valueClass)
    recordStreamUsage(streamId)
    // check if user has READ permission on the stream to make sure we fail early. this is done after we record stream
    // usage since we want to record the intent
    authorizationEnforcer.enforce(streamId, authenticationContext.getPrincipal, Action.READ)
    rdd.map(t => (t._1.get(), t._2))
  }

  override def saveAsDataset[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)], datasetName: String,
                                                       arguments: Map[String, String]): Unit = {
    saveAsDataset(rdd, getNamespace, datasetName, arguments)
  }

  override def saveAsDataset[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)], namespace: String, datasetName: String,
                                                       arguments: Map[String, String]): Unit = {
    saveAsDataset(rdd, namespace, datasetName, arguments, (dataset: Dataset, rdd: RDD[(K, V)]) => {
      val sc = rdd.sparkContext
      dataset match {
        case outputFormatProvider: OutputFormatProvider =>
          val conf = new Configuration(runtimeContext.getConfiguration)

          ConfigurationUtil.setAll(outputFormatProvider.getOutputFormatConfiguration, conf)
          conf.set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, outputFormatProvider.getOutputFormatClassName)

          saveAsNewAPIHadoopDataset(sc, conf, rdd)

        case batchWritable: BatchWritable[_, _] =>
          submitDatasetWriteJob(rdd, namespace, datasetName, arguments, (dataset: Dataset) => {
            val writable = dataset.asInstanceOf[BatchWritable[K, V]]
            (tuple: (K, V)) => writable.write(tuple._1, tuple._2)
          })
        case _ =>
          throw new IllegalArgumentException("Dataset is neither a OutputFormatProvider nor a BatchWritable")
      }
    })
  }

  override def getDataTracer(tracerName: String): DataTracer = new SparkDataTracer(runtimeContext, tracerName)

  override def getTriggeringScheduleInfo: Option[TriggeringScheduleInfo] = Option(runtimeContext.getTriggeringScheduleInfo)

  /**
    * Returns a [[org.apache.spark.broadcast.Broadcast]] of [[java.net.URI]] for
    * the http service running in the driver process.
    */
  protected[spark] def getDriveHttpServiceBaseURI(sc: SparkContext): Broadcast[URI] = {
    AbstractSparkExecutionContext.getDriveHttpServiceBaseURI(sc, sparkDriveHttpService.getBaseURI)
  }

  /**
    * Creates a factory function for creating `SparkMetricsWriter` from task context. It is expected
    * for sub-class to override to provide different implementation.
    */
  protected[spark] def createSparkMetricsWriterFactory(): (TaskContext) => SparkMetricsWriter = {
    (context: TaskContext) => {
      new SparkMetricsWriter {
        override def incrementRecordWrite(count: Int): Unit = {
          // no-op
        }
      }
    }
  }

  /**
    * Creates a [[co.cask.cdap.app.runtime.spark.DatasetCompute]] for the DatasetRDD to use.
    */
  protected[spark] def createDatasetCompute(): DatasetCompute = {
    new DatasetCompute {
      override def apply[T: ClassTag](namespace: String, datasetName: String,
                                      arguments: Map[String, String], f: (Dataset) => T): T = {
        val result = new Array[T](1)

        // For RDD to compute partitions, a transaction is needed in order to gain access to dataset instance.
        // It should either be using the active transaction (explicit transaction), or create a new transaction
        // but leave it open so that it will be used for all stages in same job execution and get committed when
        // the job ended.
        transactional.execute(new SparkTxRunnable {
          override def run(context: LineageDatasetContext) = {
            val dataset: Dataset = context.getDataset(namespace, datasetName, arguments, AccessType.READ)
            try {
              result(0) = f(dataset)
            } finally {
              context.releaseDataset(dataset)
            }
          }
        }, TransactionType.IMPLICIT_COMMIT_ON_JOB_END)
        result(0)
      }
    }
  }

  /**
    * Saves a given `RDD` to a `Dataset`.
    *
    * @param rdd the rdd to save
    * @param namespace namespace of the dataset
    * @param datasetName name of the dataset
    * @param arguments arguments for the dataset
    * @param submitWriteFunction a function to be executed from the write transaction. The function will be called in
    *                            the driver process and is typically expected it to call submit the actual write job to
    *                            Spark via the `submitDatasetWriteJob` or the `SparkContext.saveAsNewAPIHadoopDataset`
    *                            methods.
    * @tparam T type of data in the RDD
    */
  protected[spark] def saveAsDataset[T](rdd: RDD[T],
                                        namespace: String,
                                        datasetName: String,
                                        arguments: Map[String, String],
                                        submitWriteFunction: (Dataset, RDD[T]) => Unit): Unit = {
    transactional.execute(new SparkTxRunnable {
      override def run(context: LineageDatasetContext) = {
        val dataset: Dataset = context.getDataset(namespace, datasetName, arguments, AccessType.WRITE)
        val outputCommitter = dataset match {
          case outputCommitter: DatasetOutputCommitter => Some(outputCommitter)
          case _ => None
        }

        try {
          submitWriteFunction(dataset, rdd)
          outputCommitter.foreach(_.onSuccess())
        } catch {
          case t: Throwable =>
            outputCommitter.foreach(_.onFailure())
            throw t
        }
      }
    }, TransactionType.IMPLICIT)
  }

  /**
    * Submits a job to write a [[org.apache.spark.rdd.RDD]] to a [[co.cask.cdap.api.dataset.Dataset]] record
    * by record from the RDD.
    *
    * @param rdd the `RDD` to read from
    * @param namespace namespace of the dataset
    * @param datasetName name of the dataset
    * @param arguments dataset arguments
    * @param sc the `SparkContext` for submitting the job
    * @param recordWriterFactory a factory function that will be used in the executor closure for creating a writer
    *                            function that writes records from the RDD. This function has to be serializable.
    * @tparam T type of data in the RDD
    */
  protected[spark] def submitDatasetWriteJob[T](rdd: RDD[T],
                                                namespace: String,
                                                datasetName: String,
                                                arguments: Map[String, String],
                                                recordWriterFactory: (Dataset) => (T) => Unit): Unit = {
    val sc = rdd.sparkContext
    val txServiceBaseURI = getDriveHttpServiceBaseURI(sc)
    val metricsWriterFactory = createSparkMetricsWriterFactory()

    sc.runJob(rdd, (context: TaskContext, itor: Iterator[T]) => {
      // This executes in the Exeuctor
      val sparkTxClient = new SparkTransactionClient(txServiceBaseURI.value)
      val metricsWriter = metricsWriterFactory(context)
      val datasetCache = SparkRuntimeContextProvider.get().getDatasetCache
      val dataset: Dataset = datasetCache.getDataset(namespace, datasetName,
                                                     arguments, true, AccessType.WRITE)
      try {
        // Creates an Option[TransactionAware] if the dataset is a TransactionAware
        val txAware = dataset match {
          case txAware: TransactionAware => Some(txAware)
          case _ => None
        }

        // Try to get the transaction for this stage. Hardcoded the timeout to 10 seconds for now
        txAware.foreach(_.startTx(sparkTxClient.getTransaction(context.stageId(), 10, TimeUnit.SECONDS)))

        // Write through record writer created by the recordWriterFactory function
        val writer = recordWriterFactory(dataset)
        var records = 0
        while (itor.hasNext) {
          val record = itor.next()
          writer(record)
          metricsWriter.incrementRecordWrite(1)

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
    })

  }

  private def configureStreamInput(configuration: Configuration, streamId: StreamId, startTime: Long,
                                   endTime: Long, formatSpec: Option[FormatSpecification]): Configuration = {
    val streamConfig = runtimeContext.getStreamAdmin.getConfig(streamId)
    val streamPath = StreamUtils.createGenerationLocation(streamConfig.getLocation,
                                                          StreamUtils.getGeneration(streamConfig))
    AbstractStreamInputFormat.setStreamId(configuration, streamId)
    AbstractStreamInputFormat.setTTL(configuration, streamConfig.getTTL)
    AbstractStreamInputFormat.setStreamPath(configuration, streamPath.toURI)
    AbstractStreamInputFormat.setTimeRange(configuration, startTime, endTime)
    // Either use the identity decoder or use the format spec to decode
    formatSpec.fold(
      AbstractStreamInputFormat.inferDecoderClass(configuration, classOf[StreamEvent])
    )(
      spec => AbstractStreamInputFormat.setBodyFormatSpecification(configuration, spec)
    )
    configuration
  }

  private def recordStreamUsage(streamId: StreamId): Unit = {
    val oldStreamId = streamId.toId

    // Register for stream usage for the Spark program
    val oldProgramId = runtimeContext.getProgram.getId
    val owners = List(oldProgramId)
    try {
      runtimeContext.getStreamAdmin.register(owners, oldStreamId.toEntityId)
      runtimeContext.getStreamAdmin.addAccess(oldProgramId.run(getRunId.getId), oldStreamId.toEntityId,
        AccessType.READ)
    }
    catch {
      case e: Exception =>
        LOG.warn("Failed to register usage of {} -> {}", streamId, owners, e)
    }
  }

  /**
    * Saves a [[org.apache.spark.rdd.RDD]] to a dataset.
    */
  protected def saveAsNewAPIHadoopDataset[K: ClassTag, V: ClassTag](sc: SparkContext, conf: Configuration,
                                                                    rdd: RDD[(K, V)]): Unit

  /**
    * Creates a new [[co.cask.cdap.api.spark.dynamic.SparkInterpreter]].
    *
    * @param settings settings for the interpreter created. It has the classpath property being populated already.
    * @param classDir the directory to write the compiled class files
    * @param urlAdder a [[co.cask.cdap.app.runtime.spark.dynamic.URLAdder]] for adding URL to have classes
    *                 visible for the Spark executor.
    * @param onClose function to call on closing the [[co.cask.cdap.api.spark.dynamic.SparkInterpreter]]
    * @return a new instance of [[co.cask.cdap.api.spark.dynamic.SparkInterpreter]]
    */
  protected def createInterpreter(settings: Settings, classDir: File,
                                  urlAdder: URLAdder, onClose: () => Unit): SparkInterpreter
}

/**
  * Companion object for holding static fields and methods.
  */
object AbstractSparkExecutionContext {

  private val LOG = LoggerFactory.getLogger(classOf[AbstractSparkExecutionContext])
  private var driverHttpServiceBaseURI: Option[Broadcast[URI]] = None

  /**
    * Creates a [[org.apache.spark.broadcast.Broadcast]] for the base URI
    * of the [[co.cask.cdap.app.runtime.spark.SparkDriverHttpService]]
    */
  private def getDriveHttpServiceBaseURI(sc: SparkContext, baseURI: URI): Broadcast[URI] = {
    this.synchronized {
      this.driverHttpServiceBaseURI match {
        case Some(uri) => uri
        case None =>
          val broadcast = sc.broadcast(baseURI)
          driverHttpServiceBaseURI = Some(broadcast)
          broadcast
      }
    }
  }
}
