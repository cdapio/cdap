.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015-2016 Cask Data, Inc.

.. _artifacts:

=========
Artifacts
=========

.. highlight:: java

An *Artifact* is a JAR file that contains Java classes and resources required to create and run an *Application*.
*Artifacts* are identified by a *name*, *version*, and *scope*.

The **artifact name** must consist only of alphanumeric, ``'-'`` (hyphen), and ``'_'`` (underscore) characters.
For example, ``'my-application'`` is a valid artifact name, but ``'my:application'`` is not.

The **artifact version** is of the format ``'[major].[minor].[fix](-\|.)[suffix]'``. Minor, fix, and suffix
portions of the version are optional, though it is suggested that you have them conform to
standard versioning schemes. The major, minor, and fix portions of the version must be numeric.
The suffix can be any of the acceptable characters. For example, ``'3.2.0-SNAPSHOT'`` is a valid artifact version,
with a major version of 3, minor version of 2, fix version of 0, and suffix of ``SNAPSHOT``. 

The **artifact scope** is either ``'user'`` or ``'system'``. An artifact in the *'user'* scope is added by users
through the CLI or RESTful API. A *'user'* artifact belongs in a namespace and cannot be accessed in
another namespace. A *'system'* artifact is an artifact that is available across all namespaces. It
is added by placing the artifact in a special directory on either the CDAP Master node(s) or the
CDAP Standalone SDK. 

Once added to CDAP, an *Artifact* cannot be modified unless it is a **snapshot artifact.**
An *Artifact* is a snapshot artifact if the version suffix begins with ``SNAPSHOT``. For example,
``'1.2.3.SNAPSHOT-test'`` is a snapshot version because it has a suffix of ``'SNAPSHOT-test'``, which
begins with ``SNAPSHOT``. ``'1.2.3'`` is not a snapshot version because there is no suffix. ``'1.2.3-hadoop2'``
is also not a snapshot version because the suffix does not begin with ``SNAPSHOT``.

Artifacts are managed using the :ref:`Artifact HTTP RESTful APIs <http-restful-api-artifact>`.

Deploying an Artifact
=====================
An artifact is deployed through the :ref:`RESTful API <http-restful-api-artifact-add>`. 
If it contains an Application class, the artifact
can then be used to create applications. Once an artifact is deployed, it cannot be changed, with
the exception of snapshot versions of artifacts. Snapshot artifacts can be deployed multiple times,
with each deployment overwriting the previous artifact. If a program is using a snapshot artifact,
changes made to the artifact are picked up when the program is started. Once a program has started,
it is unaffected by changes made to the artifact.

Plugin Artifacts
================
Sometimes an application class exposes an interface that it expects other artifacts to implement.
For example, CDAP ships with a ``cdap-data-pipeline`` artifact that can be used to create data pipeline applications.
The artifact exposes a ``batchsource`` interface that it expects others to implement.
The ``core-plugins`` artifact contains several plugins that implement that interface. There is one source
for databases, another for HDFS files, etc. To make plugins in one artifact available to
another artifact, the plugin artifact must specify its parent artifacts. All of those parent artifacts
will then be able to use those plugins. 

Deleting an Artifact
====================
Though artifacts cannot be modified once deployed, they can be deleted. Artifact deletion is an advanced
feature and is only meant to be used if there was some error deploying the artifact. When an artifact is
deleted, any application that is configured to use the artifact will be unable to start their programs.
If a program is already running, it will be unaffected. If that running program is stopped, it will not
be able to start again until the artifact is replaced, or the application is updated to use another
artifact.

User and System Artifacts
=========================
Normally, an artifact is added to a specific namespace. Users in one namespace cannot see or use
artifacts in another namespace. These are referred to as :ref:`user artifacts <plugins-deployment-user>`.

Sometimes there is a need to provide an artifact that can be used across namespaces. One
example of this are the :ref:`Hydrator artifacts <cask-hydrator-plugins>` shipped with 
CDAP. In such scenarios, a :ref:`system artifact <plugins-deployment-system>` can be used. 

System artifacts cannot be added through the RESTful API, but must be added by placing the
artifact in a special directory. For Distributed CDAP, this directory is defined by the
``app.artifact.dir`` setting in :ref:`cdap-site.xml <appendix-cdap-site.xml>`. Multiple directories
can be defined by separating them with a semicolon. It defaults to
``/opt/cdap/master/artifacts``. For the CDAP Standalone SDK, the directory is set to the
``artifacts`` directory.

Any artifact in the directory will be added to CDAP when it starts up. In addition, a 
:ref:`RESTful API <http-restful-api-artifact-system-load>`
call can be made to scan the directory for any new artifacts that may have been added since CDAP
started. 

If a system artifact contains plugins that extend another system artifact, a matching
JSON config file must be provided to specify which artifacts it extends. In addition, if a system
artifact is a third-party JAR, the plugins in the artifact can be explicitly listed in that same config
file. 

.. highlight:: json

For example, suppose you want to add ``mysql-connector-java-5.1.3.jar`` as a system artifact. The
artifact is the MySQL JDBC driver, and is a third-party JAR that we want to use as a JDBC plugin for
the ``cdap-data-pipeline`` artifact. You would place the JAR file in the artifacts directory along with a
matching config file named ``mysql-connector-java-5.1.3.json``. The config file would contain::

  {
    "parents": [ "cdap-data-pipeline[3.2.0,4.0.0)" ],
    "plugins": [
      {
        "name": "mysql",
        "type": "jdbc",
        "description": "MYSQL JDBC external plugin",
        "className": "com.mysql.jdbc.Driver"
      }
    ]
  }

This config file specifies that the artifact can be used by versions 3.2.0 (inclusive) to 4.0.0 (exclusive)
of the cdap-data-pipeline artifact. It also specifies that there is one plugin of type ``jdbc`` and name
``mysql`` with class ``com.mysql.jdbc.Driver``. Once added, this system artifact would be usable by
applications in all namespaces.

.. highlight:: java

Example Use Case: Configurable Applications
===========================================
We will now walk through an example use case in order to illustrate how artifacts are used.
In this example, we decide to implement an application class that reads from a stream and writes
to a table using a flow. The stream that it reads from |---| and the table that it writes to |---| will be configurable.
Our development team writes code such as::

  public class MyApp extends AbstractApplication<MyApp.MyConfig> {
  
    public static class MyConfig extends Config {
      private String stream;
      private String table;
  
      private MyConfig() {
        this.stream = "A";
        this.table = "X";
      }
    }
  
    public void configure() {
      MyConfig config = getContext().getConfig();
      addStream(new Stream(config.stream));
      createDataset(config.table, Table.class);
      addFlow(new MyFlow(config.stream, config.table, config.flowConfig));
    }
  }
  
  public class MyFlow implements Flow {
    private String stream;
    private String table;
  
    MyFlow(String stream, String table) {
      this.stream = stream;
      this.table = table;
    }
  
    @Override
    public void configure() {
      setName("MyFlow");
      setDescription("Reads from a stream and writes to a table");
      addFlowlet("reader", new Reader(table));
      connectStream(stream, "reader");
    }
  }
 
  public class Reader extends AbstractFlowlet {
    @Property
    private String tableName;
    private Table table;
   
    Reader(String tableName) {
      this.tableName = tableName;
    }  

    @Override
    public void configure(FlowletConfigurer configurer) {
      useDatasets(tableName);
    }
 
    @Override
    public void initialize(FlowletContext context) throws Exception {
      table = context.getDataset(tableName);
    }
 
    @ProcessInput
    public void process(StreamEvent event) {
      Put put = new Put(Bytes.toBytes(event.getHeaders().get(config.rowkey)));
      put.add("timestamp", event.getTimestamp());
      put.add("body", Bytes.toBytes(event.getBody()));
      table.put(put);
    }
  }

.. highlight:: console

Our build system creates a JAR named ``myapp-1.0.0.jar`` that contains the ``MyApp`` class.
The JAR is deployed via the RESTful API::

  curl localhost:10000/v3/namespaces/default/artifacts/myapp --data-binary @myapp-1.0.0.jar

CDAP determines the version is 1.0.0 by examining the manifest file contained in the JAR.
Information about the artifact and the application class in the artifact are now visible
through JAR API calls::

  curl localhost:10000/v3/namespaces/default/artifacts?scope=user
  [ 
    { "name": "myapp", "scope":"USER",  "version": "1.0.0" }
  ]

  curl localhost:10000/v3/namespaces/default/artifacts/myapp/versions/1.0.0
  {
    "classes": {
      "apps": [
        {
          "className": "com.company.example.MyApp",
          "configSchema": {
            "fields": [
              { "name": "stream", "type": [ "string", "null" ] },
              { "name": "table", "type": [ "string", "null" ] }
            ],
            "name": "com.company.example.MyApp$MyConfig",
            "type": "record"
          },
          "description": ""
        }
      ],
      "plugins": []
    },
    "name": "myapp",
    "scope": "USER",
    "version": "1.0.0"
  }

With this information, a separate deployment team is able to see that the artifact contains
an application class, and it contains a config that takes in a value for ``stream`` and ``table``.
From this information, we decide to create an application named ``purchaseDump`` that reads
from the ``purchases`` stream and writes to the ``events`` table::

  curl -X PUT localhost:10000/v3/namespaces/default/apps/purchaseDump -H 'Content-Type: application/json' -d '
  { 
    "artifact": {
      "name": "myapp",
      "version": "1.0.0",
      "scope": "user"
    },
    "config": {
      "stream": "purchases",
      "table": "events"
    }
  }' 

We can then manage the lifecycle of the flow using the 
:ref:`Application Lifecycle RESTful APIs <http-restful-api-lifecycle>`.
After it has been running for a while, a bug is found in the code. The development team provides
a fix, and ``myapp-1.0.1.jar`` is released. The artifact is deployed::

  curl localhost:10000/v3/namespaces/default/artifacts/myapp --data-binary @myapp-1.0.1.jar

A call can be made to find all applications that use the old artifact::

  curl localhost:10000/v3/namespaces/default/apps?artifactName=myapp&artifactVersion=1.0.0
  [
    {
      "name": "purchaseDump",
      "artifact": {
        "name": "myapp",
        "version": "1.0.0",
        "scope": "user"
      },
      ...
    }
  ]

The flow for the ``purchaseDump`` application is stopped, then the application is updated::

  curl localhost:10000/v3/namespaces/default/apps/purchaseDump/update -d '
  {
    "artifact": {
      "name": "myapp",
      "version": "1.0.1",
      "scope": "user"
    },
    "config": {
      "stream": "purchases",
      "table": "events"
    }
  }'

The flow is started again, which picks up the new code. We quickly realize version 1.0.1 has a serious
bug and decide to roll back to the previous version. The flow is stopped and another update call is made::

  curl localhost:10000/v3/namespaces/default/apps/purchaseDump/update -d '
  {
    "artifact": {
      "name": "myapp",
      "version": "1.0.0",
      "scope": "user"
    },
    "config": {
      "stream": "purchases",
      "table": "events"
    }
  }'

Once the development team has resolved that serious bug, we can try re-deploying again...
