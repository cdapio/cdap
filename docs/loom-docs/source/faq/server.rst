:orphan:

.. _faq_toplevel:

.. index::
   single: FAQ: Loom Server
============================
Loom Server
============================

How many concurrent provisioning jobs can Loom handle?
------------------------------------------------------
Loom Server is built upon Netty. It's highly asynchronous. We have tested it to handle tens of thousands of concurrent requests.
However, we believe that there is lot of room for improvement. We are committed on improving its performance in the future releases.

Can I scale-up or scale-down a cluster?
----------------------------------------
Yes, you can scale-up and scale-down a cluster, but because of inadequate testing, this 
feature has been disabled. It will be available in the next version.

Do I have the ability to import and export configurations from one cluster to another?
----------------------------------------------------------------------------------------
Yes, you can import and export the meta data of cluster templates. Please refer to web service 
section for more information.

Where are the configurations of cluster template and it's metadata stored?
----------------------------------------------------------------------------
Cluster templates and their associated metadata are stored in Zookeeper, thereby enabling them 
to be highly available.

How do I setup a database for Loom to use it?
------------------------------------------
Loom Server persists runtime information of provisioned clusters in a relational database. 
If you are running Loom Server in the default mode, it's using the embedded Derby DB for storing all 
the runtime metadata. We don't recommend running an embedded derby DB in production. 
Loom Server can be configured to connect to standard relational database like MySQL or PostgreSQL
by simple configurations specified in loom.xml.

Following are the configuration required for enabling Loom Server to connect to external relational database:
::
 ...
 <property>
    <name>loom.jdbc.driver</name>
    <value>com.mysql.jdbc.Driver</value>
    <description>specifies db driver</description>
  </property>
  <property>
    <name>loom.jdbc.connection.string</name>
    <value>jdbc:mysql://127.0.0.1:3306/loom?useLegacyDatetimeCode=false</value>
    <description>specifies how to connect to mysql</description>
  </property>
  <property>
    <name>loom.db.user</name>
    <value>loom</value>
    <description>mysql user</description>
  </property>
  <property>
    <name>loom.db.password</name>
    <value>looming</value>
    <description>mysql user password</description>
  </property>
  ...

Is node pooling supported?
----------------------------
Yes and No. It's implemented. However, because of lack of extensive testing, this feature has been disabled in this release. 

What is node pooling?
-----------------------
Node pooling is an advanced feature that allows the clusters to be provisoned instantenously from a pool 
of pre-provisioned nodes. Pre-provisioning includes creation and installation of software components. 
Steps for configuring the cluster and its node are done once the user has requested a cluster to be materialized. 
Administrators will have the ability to enable this feature on a template by template basis. 
Node pooling increases the overall experience of the user for provisioning clusters.

Can I run multiple servers concurrently for HA?
-----------------------------------------------
Not in this release. It's being currently worked on and will be available in the next release.

Can I look at the plan before the cluster is being provisioned?
-----------------------------------------------------------------
Currently, we don't have the ability inspect the plan and cluster layout before 
the cluster provisioning is initiated. Cluster layout and plan of execution are 
available once the provisioning cycle has been initiated. 

Is there a way to plugin my own planner or layout solver?
-----------------------------------------------------------
Unfortunately, it is not available in this release. The next release will support the ability to plugin your 
very own layout solver.

Is there anyway to inspect the plan for cluster being provisioned?
--------------------------------------------------------------------
There is web service endpoint for retrieving the plan for the cluster being provisioned. The plan includes actions
that are executed on the node. Actions are divided into stages. An action in each stage can be executed in parallel.
Loom server implements a distributed barrier at each stage ensuring that the planned stage actions are all completed
before proceeding to the next stage. This ensures the actions are executed in the right dependency order.

Following is an example web service call along with the output returned from the Loom Sever provisioning a web server
on a single node.:
::
  $ curl -H 'X-Loom-UserID:<user id>' http://<loom-host-name>:<loom-host-port>/v1/loom/clusters/<cluster-id>/plans
  $ [{
        "action": "SOLVE_LAYOUT",
        "clusterId": "00000071",
        "currentStage": 0,
        "id": "00000071-001",
        "stages": []
    },{
        "action": "CLUSTER_CREATE",
        "clusterId": "00000071",
        "currentStage": 7,
        "id": "00000071-002",
        "stages": [
            [
                {
                    "id": "00000071-002-001",
                    "nodeId": "17f87422-56d5-4591-9461-5ea02e5d4c42",
                    "service": "",
                    "taskName": "CREATE"
                }
            ],
            [
                {
                    "id": "00000071-002-002",
                    "nodeId": "17f87422-56d5-4591-9461-5ea02e5d4c42",
                    "service": "",
                    "taskName": "CONFIRM"
                }
            ],
            [
                {
                    "id": "00000071-002-003",
                    "nodeId": "17f87422-56d5-4591-9461-5ea02e5d4c42",
                    "service": "",
                    "taskName": "BOOTSTRAP"
                }
            ],
            [
                {
                    "id": "00000071-002-004",
                    "nodeId": "17f87422-56d5-4591-9461-5ea02e5d4c42",
                    "service": "apache-httpd",
                    "taskName": "INSTALL"
                }
            ],
            [
                {
                    "id": "00000071-002-005",
                    "nodeId": "17f87422-56d5-4591-9461-5ea02e5d4c42",
                    "service": "firewall",
                    "taskName": "CONFIGURE"
                }
            ],
            [
                {
                    "id": "00000071-002-007",
                    "nodeId": "17f87422-56d5-4591-9461-5ea02e5d4c42",
                    "service": "hosts",
                    "taskName": "CONFIGURE"
                }
            ],
            [
                {
                    "id": "00000071-002-006",
                    "nodeId": "17f87422-56d5-4591-9461-5ea02e5d4c42",
                    "service": "apache-httpd",
                    "taskName": "CONFIGURE"
                }
            ],
            [
                {
                    "id": "00000071-002-008",
                    "nodeId": "17f87422-56d5-4591-9461-5ea02e5d4c42",
                    "service": "apache-httpd",
                    "taskName": "START"
                }
            ]
        ]
    },
  ]

