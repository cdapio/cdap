/*
 * Copyright © 2016-2021 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime.spark

import io.cdap.cdap.api._
import io.cdap.cdap.api.app.ApplicationSpecification
import io.cdap.cdap.api.data.batch.BatchWritable
import io.cdap.cdap.api.data.batch.DatasetOutputCommitter
import io.cdap.cdap.api.data.batch.OutputFormatProvider
import io.cdap.cdap.api.data.batch.Split
import io.cdap.cdap.api.dataset.Dataset
import io.cdap.cdap.api.lineage.field.Operation
import io.cdap.cdap.api.messaging.MessagingContext
import io.cdap.cdap.api.metadata.Metadata
import io.cdap.cdap.api.metadata.MetadataEntity
import io.cdap.cdap.api.metadata.MetadataScope
import io.cdap.cdap.api.metrics.Metrics
import io.cdap.cdap.api.plugin.PluginContext
import io.cdap.cdap.api.preview.DataTracer
import io.cdap.cdap.api.schedule.TriggeringScheduleInfo
import io.cdap.cdap.api.security.store.SecureStore
import io.cdap.cdap.api.spark.JavaSparkExecutionContext
import io.cdap.cdap.api.spark.SparkExecutionContext
import io.cdap.cdap.api.spark.SparkSpecification
import io.cdap.cdap.api.spark.dynamic.SparkInterpreter
import io.cdap.cdap.api.workflow.WorkflowInfo
import io.cdap.cdap.api.workflow.WorkflowToken
import io.cdap.cdap.app.runtime.spark.SparkTransactional.TransactionType
import io.cdap.cdap.app.runtime.spark.data.DatasetRDD
import io.cdap.cdap.app.runtime.spark.dynamic.AbstractSparkCompiler
import io.cdap.cdap.app.runtime.spark.dynamic.URLAdder
import io.cdap.cdap.app.runtime.spark.preview.SparkDataTracer
import io.cdap.cdap.app.runtime.spark.service.DefaultSparkHttpServiceContext
import io.cdap.cdap.app.runtime.spark.service.SparkHttpServiceServer
import io.cdap.cdap.common.conf.ConfigurationUtil
import io.cdap.cdap.data.LineageDatasetContext
import io.cdap.cdap.data2.metadata.lineage.AccessType
import io.cdap.cdap.internal.app.runtime.DefaultTaskLocalizationContext
import io.cdap.cdap.common.lang.jar.BundleJarUtil
import org.apache.hadoop.conf.Configuration
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
import java.lang
import java.net.URI
import java.net.URL
import java.util
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.tools.nsc.Settings

/**
  * Default implementation of [[io.cdap.cdap.api.spark.SparkExecutionContext]].
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
  private val transactional = new SparkTransactional(runtimeContext)
  private val workflowInfo = Option(runtimeContext.getWorkflowInfo)
  private val sparkTxHandler = new SparkTransactionHandler(runtimeContext.getTransactionSystemClient)
  private val sparkDriveHttpService = new SparkDriverHttpService(runtimeContext.getProgramName,
                                                                 runtimeContext.getHostname,
                                                                 sparkTxHandler)
  private val applicationEndLatch = new CountDownLatch(1)
  private val accessEnforcer = runtimeContext.getAccessEnforcer
  private val authenticationContext = runtimeContext.getAuthenticationContext
  private val interpreterCount = new AtomicInteger(0)

  // Optionally add the event logger based on the cConf in the runtime context
  private val eventLoggerCloseable = SparkRuntimeUtils.addEventLoggingListener(runtimeContext)

  @volatile
  private var sparkHttpServiceServer: Option[SparkHttpServiceServer] = None

  // Start the Spark driver http service
  sparkDriveHttpService.startAndWait()

  // Set the spark.repl.class.uri that points to the http service if spark-repl is present
  try {
    sparkClassLoader.loadClass(SparkRuntimeContextProvider.EXECUTOR_CLASSLOADER_NAME)
    SparkRuntimeEnv.setProperty("spark.repl.class.uri", sparkDriveHttpService.getBaseURI.toString)
  } catch {
    case _: ClassNotFoundException => // no-op
  }

  // Add a SparkListener for events from SparkContext.
  SparkRuntimeEnv.addSparkListener(new SparkListener {

    override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
      // Start the SparkHttpServiceServer to host SparkHttpServiceHandler
      val handlers = getSpecification.getHandlers
      if (!handlers.isEmpty) {
        val httpServer = new SparkHttpServiceServer(
          runtimeContext, new DefaultSparkHttpServiceContext(AbstractSparkExecutionContext.this))
        httpServer.startAndWait()
        sparkHttpServiceServer = Some(httpServer)
      }
    }

    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
      sparkHttpServiceServer.foreach(_.stopAndWait())
      applicationEndLatch.countDown
    }

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

  /**
    * If the Spark program has [[io.cdap.cdap.api.spark.service.SparkHttpServiceHandler]], block
    * until the [[org.apache.spark.SparkContext]] is being closed explicitly (on shutdown).
    */
  def waitForSparkHttpService(): Unit = {
    if (!getSpecification.getHandlers.isEmpty) {
      applicationEndLatch.await()
    }
  }

  override def close() = {
    try {
      // If there is a SparkContext, wait for the ApplicationEnd callback
      // This make sure all jobs' transactions are committed/invalidated
      SparkRuntimeEnv.stop().foreach(sc => applicationEndLatch.await())
    } finally {
      try {
        sparkDriveHttpService.stopAndWait()
      } finally {
        eventLoggerCloseable.close()
      }
    }
  }

  override def getApplicationSpecification: ApplicationSpecification = runtimeContext.getApplicationSpecification

  override def isFeatureEnabled(name: String): Boolean = runtimeContext.isFeatureEnabled(name)

  override def getClusterName: String = runtimeContext.getClusterName

  override def getRuntimeArguments: util.Map[String, String] = runtimeContext.getRuntimeArguments

  override def getRunId: RunId = runtimeContext.getRunId

  override def getNamespace: String = runtimeContext.getProgram.getNamespaceId

  override def getAdmin: Admin = runtimeContext.getAdmin

  override def getSpecification: SparkSpecification = runtimeContext.getSparkSpecification

  override def getLogicalStartTime: Long = runtimeContext.getLogicalStartTime

  override def getTerminationTime: Long = runtimeContext.getTerminationTime

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
    val classDir = createInterpreterOutputDir(interpreterCount.incrementAndGet())

    // Setup classpath and classloader
    val settings = AbstractSparkCompiler.setClassPath(new Settings())

    val urlAdder = new URLAdder {
      override def addURLs(urls: URL*) = {
        urls.foreach(u => BundleJarUtil.unJarOverwrite(new File(u.toURI), classDir))
      }
    }

    // Creates the interpreter. The cleanup is just to remove it from the cleanup manager
    var interpreterRef: Option[SparkInterpreter] = None
    val interpreter = createInterpreter(settings, classDir, urlAdder)
    interpreterRef = Some(interpreter)

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

  override def getMetadata(metadataEntity: MetadataEntity): util.Map[MetadataScope, Metadata] = {
    return runtimeContext.getMetadata(metadataEntity)
  }

  override def getMetadata(scope: MetadataScope, metadataEntity: MetadataEntity): Metadata = {
    return runtimeContext.getMetadata(scope, metadataEntity)
  }

  override def addProperties(metadataEntity: MetadataEntity, properties: util.Map[String, String]): Unit = {
    runtimeContext.addProperties(metadataEntity, properties)
  }

  override def addTags(metadataEntity: MetadataEntity, tags: String*): Unit = {
    runtimeContext.addTags(metadataEntity, tags:_*)
  }

  override def addTags(metadataEntity: MetadataEntity, tags: lang.Iterable[String]): Unit = {
    runtimeContext.addTags(metadataEntity, tags)
  }

  override def removeMetadata(metadataEntity: MetadataEntity): Unit = {
    runtimeContext.removeMetadata(metadataEntity)
  }

  override def removeProperties(metadataEntity: MetadataEntity): Unit = {
    runtimeContext.removeProperties(metadataEntity)
  }

  override def removeProperties(metadataEntity: MetadataEntity, keys: String*): Unit = {
    runtimeContext.removeProperties(metadataEntity, keys:_*)
  }

  override def removeTags(metadataEntity: MetadataEntity): Unit = {
    runtimeContext.removeTags(metadataEntity)
  }

  override def removeTags(metadataEntity: MetadataEntity, tags: String*): Unit = {
    runtimeContext.removeTags(metadataEntity, tags:_*)
  }

  override def record(operations: util.Collection[_ <: Operation]): Unit = {
    runtimeContext.record(operations)
  }

  override def flushLineage(): Unit = {
    runtimeContext.flushLineage()
  }

  override def getDataTracer(tracerName: String): DataTracer = new SparkDataTracer(runtimeContext, tracerName)

  override def getTriggeringScheduleInfo: Option[TriggeringScheduleInfo] =
    Option(runtimeContext.getTriggeringScheduleInfo)

  override def toJavaSparkExecutionContext(): JavaSparkExecutionContext =
    sparkClassLoader.createJavaExecutionContext(new SerializableSparkExecutionContext(this));

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
    * Creates a [[io.cdap.cdap.app.runtime.spark.DatasetCompute]] for the DatasetRDD to use.
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
    * Submits a job to write a [[org.apache.spark.rdd.RDD]] to a [[io.cdap.cdap.api.dataset.Dataset]] record
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

  /**
    * Saves a [[org.apache.spark.rdd.RDD]] to a dataset.
    */
  protected def saveAsNewAPIHadoopDataset[K: ClassTag, V: ClassTag](sc: SparkContext, conf: Configuration,
                                                                    rdd: RDD[(K, V)]): Unit

  /**
    * Creates a new [[io.cdap.cdap.api.spark.dynamic.SparkInterpreter]].
    *
    * @param settings settings for the interpreter created. It has the classpath property being populated already.
    * @param classDir the directory to write the compiled class files
    * @param urlAdder a [[io.cdap.cdap.app.runtime.spark.dynamic.URLAdder]] for adding URL to have classes
    *                 visible for the Spark executor.
    * @return a new instance of [[io.cdap.cdap.api.spark.dynamic.SparkInterpreter]]
    */
  protected def createInterpreter(settings: Settings, classDir: File, urlAdder: URLAdder): SparkInterpreter

  /**
    * Returns a local directory for the interpreter to use for storing dynamic class files.
    */
  protected def createInterpreterOutputDir(interpreterCount: Int): File
}

/**
  * Companion object for holding static fields and methods.
  */
object AbstractSparkExecutionContext {

  private val LOG = LoggerFactory.getLogger(classOf[AbstractSparkExecutionContext])
  private var driverHttpServiceBaseURI: Option[Broadcast[URI]] = None

  /**
    * Creates a [[org.apache.spark.broadcast.Broadcast]] for the base URI
    * of the [[io.cdap.cdap.app.runtime.spark.SparkDriverHttpService]]
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
