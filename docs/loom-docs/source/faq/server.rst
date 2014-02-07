:orphan:

.. _faq_toplevel:
.. include:: /toplevel-links.rst

============================
Loom Server
============================

How many concurrent provisioning jobs can Loom handle?
------------------------------------------------------
Loom Server is built upon Netty. It's highly asynchronous and we can tested it to handle 10s of thousand of concurrent requests.
We believe that there is lot of room for improvement here. Future releases will keep improving on the performance.

Can I scale-up or scale-down a cluster ?
----------------------------------------
Yes, you can scale-up and scale-down a cluster, but because of inadequate testing this 
feature has been disabled. It will be available in the next version.

Do I have the ability to import and export configurations from one cluster to another ?
----------------------------------------------------------------------------------------
Yes, you can import and export the meta data of cluster templates. Please refer to web service 
section for more information.

Where are the configurations of cluster template and it's metadata stored ?
----------------------------------------------------------------------------
Cluster templates and their associated metadata are stored in Zookeeper enabling them 
to highly available.

How do I setup a database for Loom to use?
------------------------------------------
Loom Server persists runtime information of clusters being provisioned in a relational database. 
If you are running Loom Server in the default mode, it's using the embedded Derby DB for storing all 
the runtime metadata. It is not recommended to be running in production with embedded derby DB. 
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

Is node pooling supported ?
----------------------------
Yes and No. It's implemented but because of not extensive testing of this specific feature, 
the capability has been disabled in this release. 

What is node pooling ?
-----------------------
Node pooling is an advanced feature that allows the clusters to be provisoned instantenously from a pool 
of pre-provisioned nodes. Preprovisioning includes creation and installation of software components. 
Steps for configuring the cluster and it's node is done once the user has requested a cluster to be materialized. 
Administrators will have the ability to enable this feature on template by template basis. Pooling increases
the overall experience of the user for provisioning clusters.

Can I run multiple servers concurrently for HA?
-----------------------------------------------
Not in this release. It's being currently worked on and would be available in the next release.

Can I look at the plan before the cluster is being provisioned ?
-----------------------------------------------------------------
Currently, we don't have the ability inspect the plan and cluster layout before 
the cluster provisioning is initiated. Cluster layout and plan of execution is 
available once the provisioning cycle has been initiated. 

Is there a way to plugin my own planner or layout solver ?
-----------------------------------------------------------
Not in this release. Next release will support the ability to plugin your very 
own layout solver.

Is there anyway to inspect the plan for cluster being provisioned ?
--------------------------------------------------------------------
There is web service endpoint for retrieving the plan for the cluster being provisioned. Plan includes actions
that are executed on the node. Actions are divided into stages. Action in each stage can be executed in parallel.
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

