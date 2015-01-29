.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _appendix-cdap-site.xml:

============================================
Appendix: ``cdap-site.xml``
============================================

Here are the parameters that can be defined in the ``cdap-site.xml`` file,
their default values, descriptions and notes.

For information on configuring the ``cdap-site.xml`` file and CDAP for security,
see the :ref:`configuration-security` section.

..   :widths: 20 20 30

.. list-table::
   :widths: 30 35 35
   :header-rows: 1

   * - Parameter name
     - Default Value
     - Description
   * - ``app.adapter.dir``
     - Directory where all archives for adapters are stored
   * - ``app.bind.address``
     - ``127.0.0.1``
     - App-Fabric server host address
   * - ``app.bind.port``
     - ``45000``
     - App-Fabric server port
   * - ``app.command.port``
     - ``45010``
     - App-Fabric command port
   * - ``app.output.dir``
     - ``/programs``
     - Directory where all archives are stored
   * - ``app.program.jvm.opts``
     - ``${twill.jvm.gc.opts}``
     - Java options for all program containers
   * - ``app.temp.dir``
     - ``/tmp``
     - Temp directory
   * - ``dashboard.bind.port``
     - ``9999``
     - CDAP Console bind port
   * - ``dashboard.ssl.bind.port``
     - ``9443``
     - CDAP Console bind port for HTTPS
   * - ``dashboard.ssl.disable.cert.check``
     - ``false``
     - True to disable SSL certificate check from the CDAP Console
   * - ``data.local.storage``
     - ``${local.data.dir}/ldb``
     - Database directory
   * - ``data.local.storage.blocksize``
     - ``1024``
     - Block size in bytes
   * - ``data.local.storage.cachesize``
     - ``104857600``
     - Cache size in bytes
   * - ``data.queue.config.update.interval``
     - ``5``
     - Frequency, in seconds, of updates to the queue consumer
   * - ``data.queue.table.name``
     - ``queues``
     - Tablename for queues
   * - ``data.tx.bind.address``
     - ``127.0.0.1``
     - Transaction Inet address
   * - ``data.tx.bind.port``
     - ``15165``
     - Transaction bind port
   * - ``data.tx.client.count``
     - ``5``
     - Number of pooled transaction instances
   * - ``data.tx.client.provider``
     - ``thread-local``
     - Provider strategy for transaction clients
   * - ``data.tx.command.port``
     - ``15175``
     - Transaction command port number
   * - ``data.tx.janitor.enable``
     - ``true``
     - Whether or not the TransactionDataJanitor coprocessor is enabled on tables
   * - ``data.tx.server.io.threads``
     - ``2``
     - Number of transaction IO threads
   * - ``data.tx.server.threads``
     - ``25``
     - Number of transaction threads
   * - ``data.tx.snapshot.dir``
     - ``${hdfs.namespace}/tx.snapshot``
     - Directory in HDFS used to store snapshots and transaction logs
   * - ``data.tx.snapshot.interval``
     - ``300``
     - Frequency of transaction snapshots in seconds
   * - ``data.tx.snapshot.local.dir``
     - ``${local.data.dir}/tx.snapshot``
     - Snapshot storage directory on the local filesystem
   * - ``data.tx.snapshot.retain``
     - ``10``
     - Number of retained transaction snapshot files
   * - ``data.tx.timeout``
     - ``30``
     - Timeout value in seconds for a transaction; if the transaction is not finished
       in that time, it is marked invalid
   * - ``enable.unrecoverable.reset``
     - ``false``
     - **WARNING: Enabling this option makes it possible to delete all
       applications and data; no recovery is possible!**
   * - ``explore.active.operation.timeout.secs``
     - ``86400``
     - Timeout value in seconds for a SQL operation whose result is not fetched completely
   * - ``explore.cleanup.job.schedule.secs``
     - ``60``
     - Time in secs to schedule clean up job to timeout operations
   * - ``explore.enabled``
     - ``false``
     - Determines if the CDAP Explore Service is enabled
   * - ``explore.executor.container.instances``
     - ``1``
     - Number of explore executor instances
   * - ``explore.executor.max.instances``
     - ``1``
     - Maximum number of explore executor instances
   * - ``explore.inactive.operation.timeout.secs``
     - ``3600``
     - Timeout value in seconds for a SQL operation which has no more results to be fetched
   * - ``gateway.boss.threads``
     - ``1``
     - Number of Netty server boss threads
   * - ``gateway.connection.backlog``
     - ``20000``
     - Maximum connection backlog of Router
   * - ``gateway.exec.threads``
     - ``20``
     - Number of Netty server executor threads
   * - ``gateway.max.cached.events.per.stream.num``
     - ``5000``
     - Maximum number of a single stream's events cached before flushing
   * - ``gateway.max.cached.stream.events.bytes``
     - ``52428800``
     - Maximum size (in bytes) of stream events cached before flushing
   * - ``gateway.max.cached.stream.events.num``
     - ``10000``
     - Maximum number of stream events cached before flushing
   * - ``gateway.memory.mb``
     - ``2048``
     - Memory in MB for Router process in YARN
   * - ``gateway.num.cores``
     - ``2``
     - Cores requested per Router container in YARN
   * - ``gateway.num.instances``
     - ``1``
     - Number of Router instances in YARN
   * - ``gateway.stream.callback.exec.num.threads``
     - ``5``
     - Number of threads in stream events callback executor
   * - ``gateway.stream.events.flush.interval.ms``
     - ``150``
     - Interval at which cached stream events get flushed
   * - ``gateway.worker.threads``
     - ``10``
     - Number of Netty server worker threads
   * - ``hdfs.lib.dir``
     - ``${hdfs.namespace}/lib``
     - Common directory in HDFS for JAR files for coprocessors
   * - ``hdfs.namespace``
     - ``/${root.namespace}``
     - Namespace for files written by CDAP
   * - ``hdfs.user``
     - ``yarn``
     - User name for accessing HDFS
   * - ``hive.local.data.dir``
     - ``${local.data.dir}/hive``
     - Location of hive relative to ``local.data.dir``
   * - ``hive.server.bind.address``
     - ``localhost``
     - Router address hive server binds to
   * - ``kafka.bind.address``
     - ``0.0.0.0``
     - Kafka server hostname
   * - ``kafka.bind.port``
     - ``9092``
     - Kafka server port
   * - ``kafka.default.replication.factor``
     - ``1``
     - Kafka replication factor [`Note 1`_]
   * - ``kafka.log.dir``
     - ``/tmp/kafka-logs``
     - Kafka log storage directory
   * - ``kafka.num.partitions``
     - ``10``
     - Default number of partitions for a topic
   * - ``kafka.seed.brokers``
     - ``127.0.0.1:9092``
     - Kafka brokers list (comma separated)
   * - ``kafka.zookeeper.namespace``
     - ``kafka``
     - Kafka Zookeeper namespace
   * - ``local.data.dir``
     - ``data``
     - Data directory for local mode
   * - ``log.base.dir``
     - ``/logs/avro``
     - Base log directory
   * - ``log.cleanup.run.interval.mins``
     - ``1440``
     - Log cleanup interval in minutes
   * - ``log.publish.num.partitions``
     - ``10``
     - Number of Kafka partitions to publish the logs to
   * - ``log.retention.duration.days``
     - ``7``
     - Log file HDFS retention duration in days
   * - ``log.run.account``
     - ``cdap``
     - Logging service account
   * - ``log.saver.event.bucket.interval.ms``
     - ``1000``
     - Log events published in this interval (in milliseconds) will be put in one
       in-memory bucket and processed in a batch. Smaller values will increase the odds of
       log events going out-of-order.
   * - ``log.saver.event.max.inmemory.buckets``
     - ``8``
     - Maximum number of event buckets in memory.
   * - ``log.saver.num.instances``
     - ``1``
     - Log Saver instances to run in YARN
   * - ``log.saver.run.memory.megs``
     - ``1024``
     - Memory in MB allocated to the Log Saver process
   * - ``metadata.bind.address``
     - ``127.0.0.1``
     - Metadata server address
   * - ``metadata.bind.port``
     - ``45004``
     - Metadata server port
   * - ``metadata.program.run.history.keepdays``
     - ``30``
     - Number of days to keep metadata run history
   * - ``metrics.data.table.retention.resolution.1.seconds``
     - ``7200``
     - Retention resolution of the 1 second table in seconds
   * - ``metrics.kafka.partition.size``
     - ``10``
     - Number of partitions for metrics topic
   * - ``metrics.query.bind.address``
     - ``127.0.0.1``
     - Metrics query server host address
   * - ``metrics.query.bind.port``
     - ``45005``
     - Metrics query server port
   * - ``root.namespace``
     - ``cdap``
     - Namespace for this CDAP instance
   * - ``router.bind.address``
     - ``0.0.0.0``
     - Router server address
   * - ``router.bind.port``
     - ``10000``
     - Port number that the CDAP router should bind to for HTTP Connections
   * - ``router.client.boss.threads``
     - ``1``
     - Number of router client boss threads
   * - ``router.client.worker.threads``
     - ``10``
     - Number of router client worker threads
   * - ``router.connection.backlog``
     - ``20000``
     - Maximum router connection backlog
   * - ``router.server.address``
     - ``localhost``
     - Router address to which Console connects
   * - ``router.server.boss.threads``
     - ``1``
     - Number of router server boss threads
   * - ``router.server.port``
     - ``10000``
     - Router port to which Console connects
   * - ``router.server.worker.threads``
     - ``10``
     - Number of router server worker threads
   * - ``router.ssl.bind.port``
     - ``10443``
     - Port number that the CDAP router should bind to for HTTPS Connections
   * - ``scheduler.max.thread.pool.size``
     - ``30``
     - Size of the scheduler thread pool
   * - ``security.auth.server.bind.address``
     - ``127.0.0.1``
     - IP address that the CDAP Authentication Server should bind to
   * - ``security.auth.server.bind.port``
     - ``10009``
     - Port number that the CDAP Authentication Server should bind to for HTTP
   * - ``security.auth.server.ssl.bind.port``
     - ``10010``
     - Port to bind to for HTTPS on the CDAP Authentication Server
   * - ``security.authentication.basic.realmfile``
     -
     - Username / password file to use when basic authentication is configured
   * - ``security.authentication.handlerClassName``
     -
     - Name of the authentication implementation to use to validate user credentials
   * - ``security.authentication.loginmodule.className``
     -
     - JAAS LoginModule implementation to use when
       ``co.cask.security.server.JAASAuthenticationHandler`` is configured for
       ``security.authentication.handlerClassName``
   * - ``security.data.keyfile.path``
     - ``${local.data.dir}/security/keyfile``
     - Path to the secret key file (only used in single-node operation)
   * - ``security.enabled``
     - ``false``
     - Enables authentication for CDAP.  When set to ``true`` all requests to CDAP must
       provide a valid access token.
   * - ``security.realm``
     - ``cask``
     - Authentication realm used for scoping security.  This value should be unique for each
       installation of CDAP.
   * - ``security.server.extended.token.expiration.ms``
     - ``604800000``
     - Admin tool access token expiration time in milliseconds (defaults to 1 week) (internal)
   * - ``security.server.maxthreads``
     - ``100``
     - Maximum number of threads that the CDAP Authentication Server should use for
       handling HTTP requests
   * - ``security.server.token.expiration.ms``
     - ``86400000``
     - Access token expiration time in milliseconds (defaults to 24 hours)
   * - ``security.token.digest.algorithm``
     - ``HmacSHA256``
     -  Algorithm used for generating MAC of access tokens
   * - ``security.token.digest.key.expiration.ms``
     - ``3600000``
     - Time duration (in milliseconds) after which an active secret key
       used for signing tokens should be retired
   * - ``security.token.digest.keylength``
     - ``128``
     - Key length used in generating the secret keys for generating MAC of access tokens
   * - ``security.token.distributed.parent.znode``
     - ``/${root.namespace}/security/auth``
     - Parent node in ZooKeeper used for secret key distribution in distributed mode
   * - ``ssl.enabled``
     - ``false``
     - True to enable SSL
   * - ``stream.flume.port``
     - ``10004``
     -
   * - ``stream.flume.threads``
     - ``20``
     -
   * - ``thrift.max.read.buffer``
     - ``16777216``
     - Maximum read buffer size in bytes used by the Thrift server [`Note 2`_]
   * - ``twill.java.reserved.memory.mb``
     - ``250``
     - Reserved non-heap memory in MB for Twill container
   * - ``twill.jvm.gc.opts``
     - | ``-verbose:gc``
       | ``-Xloggc:<log-dir>/gc.log``
       | ``-XX:+PrintGCDetails``
       | ``-XX:+PrintGCTimeStamps``
       | ``-XX:+UseGCLogFileRotation``
       | ``-XX:NumberOfGCLogFiles=10``
       | ``-XX:GCLogFileSize=1M``
     - Java garbage collection options for all Twill containers; ``<log-dir>`` is the location
       of the log directory on each machine
   * - ``twill.no.container.timeout``
     - ``120000``
     - Amount of time in milliseconds to wait for at least one container for Twill runnable
   * - ``twill.zookeeper.namespace``
     - ``/twill``
     - Twill Zookeeper namespace prefix
   * - ``yarn.user``
     - ``yarn``
     - User name for running applications in YARN
   * - ``zookeeper.quorum``
     - ``127.0.0.1:2181/${root.namespace}``
     - Zookeeper address host:port
   * - ``zookeeper.session.timeout.millis``
     - ``40000``
     - Zookeeper session time out in milliseconds

.. _note 1:

**Note 1**:

    ``kafka.default.replication.factor`` is used to replicate *Kafka* messages across multiple
    machines to prevent data loss in the event of a hardware failure. The recommended setting
    is to run at least two *Kafka* servers. If you are running two *Kafka* servers, set this
    value to 2; otherwise, set it to the number of *Kafka* servers

.. _note 2:

**Note 2**:

    Maximum read buffer size in bytes used by the Thrift server: this value should be set to
    greater than the maximum frame sent on the RPC channel.
