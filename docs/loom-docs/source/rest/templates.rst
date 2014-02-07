:orphan:
.. include:: /toplevel-links.rst

==================
REST API: Cluster Templates
==================

Loom REST APIs allow you to create templates describing different types of clusters.  For example, there may be a template for Hadoop clusters, a template for LAMP clusters, a template for Solr clusters, etc.  Templates contain enough information that a user can simply specify a template and a number of machines, and it will be enough information to create a cluster.  This is done by first describing the set of services, hardware types, and image types that a cluster is compatible with.  Next, default values for provider, services, and configuration are given, with optional defaults for cluster-wide hardware and image type.  Finally, a set of constraints are defined which describe how services, hardware, and images should be placed on a cluster.


**Cluster Template Details**
=================

Each cluster template configured in the system will have a unique name, a short description and a section devoted to compatibilities, defaults, and constraints.

Compatibility
^^^^^^^^^^^^^

A cluster template defines 3 things in its compatibility section.  The first is a set of services that are compatible with the template.  This means that when a user goes to create a cluster with this template, he/she is allowed to specify any service from this set as services to place on the cluster.  Loom will not automatically pull in service dependencies, so the full set of compatible services must be defined. 

Next, a set of compatible hardware types is defined.  This means only hardware types in the compatible set can be used to create a cluster.  Similarly, compatible image types are defined, where only image types in the compatible set can be used to create a cluster.

Example Compatibility Section
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. code-block:: bash

    "compatibility": {
        "hardwaretypes": [
            "small",
            "medium",
            "large"
        ],
        "imagetypes": [
            "centos6",
            "ubuntu12"
        ],
        "services": [
            "hadoop-hdfs-namenode",
            "hadoop-hdfs-datanode",
            "hadoop-yarn-resourcemanager",
            "hadoop-yarn-nodemanager",
            "zookeeper-server",
            "hbase-master",
            "hbase-regionserver",
            "reactor"
        ]
    }

Defaults
^^^^^^^^

The defaults section describes what will be used to create a cluster if the user does not specifically specify anything beyond the number of machines to use in a cluster.  Everything in this section can be overwritten by the user, though it is likely only advanced users will want to do so.  Templates must contain a set of default services, a default provider, and default config.  Optionally, a hardware type to use across the entire cluster, and an image type to use across the entire cluster may be specified.  The default services must be a subset of the services defined in the compatibility section.  Similarly, if a hardwaretype or imagetype is specified, it must be one of the types given in the compatibility section.  Lastly, the config is a JSON Object that gets passed straight through to provisioners, usually describing different configuration settings for the services that will be placed on the cluster. 

Example Default Section
^^^^^^^^^^^^^^^^^^^^^^^
.. code-block:: bash

    "defaults": {
        "services": [
            "hadoop-hdfs-namenode",
            "hadoop-hdfs-datanode",
            "hadoop-yarn-resourcemanager",
            "hadoop-yarn-nodemanager"
        ],
        "provider": "rackspace",
        "hardwaretype": "medium",
        "imagetype": "ubuntu12",
        "config": {
            "hadoop": {
                "core_site": {
                    "fs.defaultFS": "hdfs://%host.service.hadoop-hdfs-namenode%"
                },
                "hdfs_site": {
                    "dfs.datanode.max.xcievers": "4096"
                },
                "mapred_site": {
                    "mapreduce.framework.name": "yarn"
                },
                "yarn_site": {
                    "yarn.resourcemanager.hostname": "%host.service.hadoop-yarn-resourcemanager%"
                }
            },
            "hbase": {
                "hbase_site": {
                    "hbase.rootdir": "hdfs://%host.service.hadoop-hdfs-namenode%/hbase",
                    "hbase.cluster.distributed": "true",
                    "hbase.zookeeper.quorum": "%join(map(host.service.zookeeper-server,'$:2181'),',')%"
                }
            }
        }
    }

Constraints
^^^^^^^^^^^
Templates can define 2 types of constraints -- layout and service.

Layout constraints define which services must and cant coexist on the same node.  Must coexist constraints are given as an array of arrays.  Each inner array is a set of services that must all coexist together on the same node.  For example, in a hadoop cluster, you generally want datanodes, regionservers, and nodemanagers to all be placed together, so to specify that you would put all 3 services in the same must coexist constraint.  Must coexist constraints are not transitive.  If there is one constraint saying serviceA must coexist with serviceB, and another constraint saying serviceB must coexist with serviceC, this does NOT mean that serviceA must coexist with serviceC.  Loom was designed this way to prevent unintended links between services, especially as the number of must coexist constraints increase.  If a must coexist rule contains a service that is not on the cluster, it is shrunk to ignore the service that is not on the cluster.  For example, your template may be compatible with datanodes, nodemanagers, and regionservers.  However, by default you only put datanodes and nodemanagers on the cluster.  A constraint stating that datanodes, nodemanagers, and regionservers must coexist on the same node will get transformed into a constraint that just says datanodes and nodemanagers must coexist on the same node.

The other type of layout constraint are cant coexist constraints, which are also given as an array of arrays.  Each inner array is a set of services that cannot all coexist together on the same node.  For example, in a hadoop cluster, you generally do not want your namenode to be on the same node as a datanode.  Specifying more than 2 services in a cant coexist rule means the entire set cannot exist on the same node.  For example, if there is a constraint that serviceA, serviceB, and serviceC cant coexist, serviceA and serviceB can still coexist on the same node.  Though supported, this can be confusing so best practice is to keep cant coexist constraints binary.  Anything not mentioned in the must or cant coexist constraints are allowed. 

Service constraints are defined as a JSON Object containing optional hardware types, image types, and quantities for a service that can be placed on the cluster.  Keys in the JSON Object are service names, and values are JSON Objects representing the service constraint.  A service constraint can contain a key named hardwaretypes, with a corresponding JSON array of hardware types that the service can be placed on.  Any node with that service must use one of the hardware types in the array.  If nothing is given, the service can go on a node with any type of hardware.  Similarly, a service constraint can contain a key named imagetypes, with a corresponding JSON array of image types that the service can be placed on.  Any node with that service must use one of the image types in the array.  If nothing is given, the service can go on a node with any type of image.  A service constraint can also limit the quantities of that service across the entire cluster.  These are specified with a quantities key, whose corresponding value is a JSON Object.  The JSON Object can optionally contain a key for min and an integer for the minimum number of nodes that must contain the service across the entire cluster.  Similarly, a max can be given to limit the maximum number of nodes with the service on the cluster.  

Example Constraints Section
^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. code-block:: bash

    "constraints": {
        "layout": {
            "mustcoexist": [
                [ "hadoop-hdfs-datanode", "hadoop-yarn-nodemanager", "hbase-regionserver" ],
                [ "hadoop-hdfs-namenode", "hadoop-yarn-resourcemanager", "hbase-master" ]
            ],
            "cantcoexist": [
                [ "hadoop-hdfs-namenode", "hadoop-hdfs-datanode" ],
                [ "hadoop-hdfs-namenode", "zookeeper-server" ],
                [ "hadoop-hdfs-datanode", "zookeeper-server" ]
            ]
        },
        "services": {
            "hadoop-hdfs-namenode": {
                "hardwaretypes": [ "large" ], 
                "quantities": {
                    "min": "1",
                    "max": "1"
                }
            },
            "hadoop-hdfs-datanode": {
                "hardwaretypes": [ "medium" ],
                "quantities": {
                    "min": "3"
                }
            }
        }
    }

.. _template-create:
**Add a Cluster Template**
==================

To create a new cluster template, make a HTTP POST request to URI:
::
 /clustertemplates

POST Parameters
^^^^^^^^^^^^^^^^

Required Parameters

.. list-table::
   :widths: 15 10
   :header-rows: 1

   * - Parameter
     - Description
   * - name
     - Specifies the name to be assigned to the cluster template that is being created. Should have only
       alphanumeric, dash(-), dot(.) & underscore(_)
   * - description
     - Provides a description for the cluster template.
   * - defaults 
     - JSON Object describing default service set, provider, config, and optional imagetype and hardwaretype. 
   * - compatibility
     - JSON Object describing services, hardware types, and imagetypes that are compatible with the cluster.
   * - constraints
     - JSON Object describing layout and service constraints.

HTTP Responses
^^^^^^^^^^^^^^

.. list-table:: 
   :widths: 15 10 
   :header-rows: 1

   * - Status Code
     - Description
   * - 200 (OK)
     - Successfully created
   * - 400 (BAD_REQUEST)
     - Bad request, server is unable to process the request or a cluster template with the name already exists 
       in the system.

Example
^^^^^^^^
.. code-block:: bash

 $ curl -X POST 
        -H 'X-Loom-UserID:admin' 
        -H 'X-Loom-ApiKey:<apikey>'
        -d '{
                "name": "hadoop.example",
                "description": "Hadoop cluster with hdfs and YARN",
                "compatibility": {
                    "hardwaretypes": [ "small", "medium", "large" ],
                    "imagetypes": [ "centos6", "ubuntu12" ],
                    "services": [
                        "hadoop-hdfs-namenode",
                        "hadoop-hdfs-datanode",
                        "hadoop-yarn-resourcemanager",
                        "hadoop-yarn-nodemanager",
                        "zookeeper-server",
                        "hbase-master",
                        "hbase-regionserver",
                        "reactor"
                    ]
                },
                "defaults": {
                    "services": [
                        "hadoop-hdfs-namenode",
                        "hadoop-hdfs-datanode",
                        "hadoop-yarn-resourcemanager",
                        "hadoop-yarn-nodemanager"
                    ],
                    "provider": "rackspace",
                    "imagetype": "ubuntu12",
                    "config": {
                        "hadoop": {
                            "core_site": {
                                "fs.defaultFS": "hdfs://%host.service.hadoop-hdfs-namenode%"
                            },
                            "hdfs_site": {
                                "dfs.datanode.max.xcievers": "4096"
                            },
                            "mapred_site": {
                                "mapreduce.framework.name": "yarn"
                            },
                            "yarn_site": {
                                "yarn.resourcemanager.hostname": "%host.service.hadoop-yarn-resourcemanager%"
                            }
                       },
                       "hbase": {
                           "hbase_site": {
                               "hbase.rootdir": "hdfs://%host.service.hadoop-hdfs-namenode%/hbase",
                               "hbase.cluster.distributed": "true",
                               "hbase.zookeeper.quorum": "%join(map(host.service.zookeeper-server,'$:2181'),',')%"
                           }
                       }
                   }   
                },
                "constraints": {
                    "layout": {
                        "mustcoexist": [
                            [ "hadoop-hdfs-datanode", "hadoop-yarn-nodemanager", "hbase-regionserver" ],
                            [ "hadoop-hdfs-namenode", "hadoop-yarn-resourcemanager", "hbase-master" ]
                        ],
                        "cantcoexist": [
                            [ "hadoop-hdfs-namenode", "hadoop-hdfs-datanode" ],
                            [ "hadoop-hdfs-namenode", "zookeeper-server" ],
                            [ "hadoop-hdfs-datanode", "zookeeper-server" ]
                        ]
                    },
                    "services": {
                        "hadoop-hdfs-namenode": {
                            "hardwaretypes": [ "large" ],
                            "quantities": {
                                "min": "1",
                                "max": "1"
                            }
                        },
                        "hadoop-hdfs-datanode": {
                            "hardwaretypes": [ "medium" ],
                            "quantities": {
                                "min": "3"
                            }
                        }
                    }
                }
            }'
        http://<loom-server>:<loom-port>/<version>/loom/clustertemplates

.. _template-retrieve:
**Retrieve a Cluster Template**
===================

To retrieve details about a cluster template, make a GET HTTP request to URI:
::
 /clustertemplates/{name}

This resource represents an individual cluster template requested to be viewed.

HTTP Responses
^^^^^^^^^^^^^^

.. list-table::
   :widths: 15 10
   :header-rows: 1

   * - Status Code
     - Description
   * - 200 (OK)
     - Successfull
   * - 404 (NOT FOUND)
     - If the resource requested is not configured and available in system.

Example
^^^^^^^^
.. code-block:: bash

 $ curl -H 'X-Loom-UserID:admin' 
        -H 'X-Loom-ApiKey:<apikey>'
        http://<loom-server>:<loom-port>/<version>/loom/clustertemplates/hadoop.example
 $ {
       "name": "hadoop.example",
       "description": "Hadoop cluster with hdfs and YARN",
       "compatibility": {
           "hardwaretypes": [ "small", "medium", "large" ],
           "imagetypes": [ "centos6", "ubuntu12" ],
           "services": [
               "hadoop-hdfs-namenode",
               "hadoop-hdfs-datanode",
               "hadoop-yarn-resourcemanager",
               "hadoop-yarn-nodemanager",
               "zookeeper-server",
               "hbase-master",
               "hbase-regionserver",
               "reactor"
           ]
       },
       "defaults": {
           "services": [
               "hadoop-hdfs-namenode",
               "hadoop-hdfs-datanode",
               "hadoop-yarn-resourcemanager",
               "hadoop-yarn-nodemanager"
           ],
           "provider": "rackspace",
           "imagetype": "ubuntu12",
           "config": {
               "hadoop": {
                   "core_site": {
                       "fs.defaultFS": "hdfs://%host.service.hadoop-hdfs-namenode%"
                   },
                   "hdfs_site": {
                       "dfs.datanode.max.xcievers": "4096"
                   },
                   "mapred_site": {
                       "mapreduce.framework.name": "yarn"
                   },
                   "yarn_site": {
                       "yarn.resourcemanager.hostname": "%host.service.hadoop-yarn-resourcemanager%"
                   }
               },
               "hbase": {
                   "hbase_site": {
                       "hbase.rootdir": "hdfs://%host.service.hadoop-hdfs-namenode%/hbase",
                       "hbase.cluster.distributed": "true",
                       "hbase.zookeeper.quorum": "%join(map(host.service.zookeeper-server,'$:2181'),',')%"
                   }
               }
          }
      },
      "constraints": {
          "layout": {
               "mustcoexist": [
                   [ "hadoop-hdfs-datanode", "hadoop-yarn-nodemanager", "hbase-regionserver" ],
                   [ "hadoop-hdfs-namenode", "hadoop-yarn-resourcemanager", "hbase-master" ]
               ],
               "cantcoexist": [
                   [ "hadoop-hdfs-namenode", "hadoop-hdfs-datanode" ],
                   [ "hadoop-hdfs-namenode", "zookeeper-server" ],
                   [ "hadoop-hdfs-datanode", "zookeeper-server" ]
               ]
          },
          "services": {
               "hadoop-hdfs-namenode": {
                   "hardwaretypes": [ "large" ],
                   "quantities": {
                       "min": "1",
                       "max": "1"
                   }
               },
               "hadoop-hdfs-datanode": {
                   "hardwaretypes": [ "medium" ],
                   "quantities": {
                       "min": "3"
                   }
               }
          }
      }
  }
.. _template-delete:
**Delete a Cluster Template**
=================

To delete a cluster template, make a DELETE HTTP request to URI:
::
 /clustertemplates/{name}

This resource represents an individual cluster template requested to be deleted.

HTTP Responses
^^^^^^^^^^^^^^

.. list-table::
   :widths: 15 10
   :header-rows: 1

   * - Status Code
     - Description
   * - 200 (OK)
     - If delete was successfull
   * - 404 (NOT FOUND)
     - If the resource requested is not found.

Example
^^^^^^^^
.. code-block:: bash

 $ curl -X DELETE
        -H 'X-Loom-UserID:admin' 
        -H 'X-Loom-ApiKey:<apikey>'
        http://<loom-server>:<loom-port>/<version>/loom/clustertemplates/hadoop.example

.. _template-modify:
**Update a Cluster Template**
==================

To update a service, make a PUT HTTP request to URI:
::
 /clustertemplates/{name}

Resource specified above respresents a individual services which is being updated.
Currently, the update of services resource requires complete services object to be
returned back rather than individual fields.

PUT Parameters
^^^^^^^^^^^^^^^^

Required Parameters

.. list-table::
   :widths: 15 10
   :header-rows: 1

   * - Parameter
     - Description
   * - name
     - Specifies the name of the cluster template to be updated.
   * - description
     - New description or old one for the cluster template.
   * - defaults 
     - JSON Object describing default service set, provider, config, and optional imagetype and hardwaretype. 
   * - compatibility
     - JSON Object describing services, hardware types, and imagetypes that are compatible with the cluster.
   * - constraints
     - JSON Object describing layout and service constraints.

HTTP Responses
^^^^^^^^^^^^^^

.. list-table::
   :widths: 15 10
   :header-rows: 1

   * - Status Code
     - Description
   * - 200 (OK)
     - If update was successfull
   * - 400 (BAD REQUEST)
     - If the resource requested is not found or the fields of the PUT body doesn't specify all the required fields.

Example
^^^^^^^^
.. code-block:: bash

 $ curl -X PUT 
        -H 'X-Loom-UserID:admin' 
        -H 'X-Loom-ApiKey:<apikey>'
        -d 
  '{
       "name": "hadoop.example",
       "description": "Reduced Hadoop cluster",
       "compatibility": {
           "hardwaretypes": [ "small" ],
           "imagetypes": [
               "centos6",
               "ubuntu12"
           ],
           "services": [
               "hadoop-hdfs-namenode",
               "hadoop-hdfs-datanode",
               "hadoop-yarn-resourcemanager",
               "hadoop-yarn-nodemanager"
           ]
       },
       "defaults": {
           "services": [
               "hadoop-hdfs-namenode",
               "hadoop-hdfs-datanode",
               "hadoop-yarn-resourcemanager",
               "hadoop-yarn-nodemanager"
           ],
           "provider": "rackspace",
           "imagetype": "ubuntu12",
           "config": {
               "hadoop": {
                   "core_site": {
                       "fs.defaultFS": "hdfs://%host.service.hadoop-hdfs-namenode%"
                   }, 
                   "hdfs_site": {
                       "dfs.datanode.max.xcievers": "4096"
                   },
                   "mapred_site": {
                       "mapreduce.framework.name": "yarn"
                   },
                   "yarn_site": {
                       "yarn.resourcemanager.hostname": "%host.service.hadoop-yarn-resourcemanager%"
                   }
               }
           }
       },
       "constraints": {
           "layout": {
               "mustcoexist": [
                   [
                       "hadoop-hdfs-datanode",
                       "hadoop-yarn-nodemanager"
                   ],
                   [
                       "hadoop-hdfs-namenode",
                       "hadoop-yarn-resourcemanager"
                   ]
               ],
               "cantcoexist": [
                   [
                       "hadoop-hdfs-namenode",
                       "hadoop-hdfs-datanode"
                   ]
               ]
           },
           "services": {
               "hadoop-hdfs-namenode": {
                   "quantities": {
                       "min": "1",
                       "max": "1"
                   }
               },
               "hadoop-hdfs-datanode": {
                   "quantities": {
                       "min": "3"
                   } 
               }
           }
       }
   }
      http://<loom-server>:<loom-port>/<version>/loom/clustertemplates/hadooop.example

.. _template-all-list:
**List all Cluster Templates**
=============================

To list all the services configured within in Loom, make GET HTTP request to URI:
::
 /clustertemplates

HTTP Responses
^^^^^^^^^^^^^^

.. list-table::
   :widths: 15 10
   :header-rows: 1

   * - Status Code
     - Description
   * - 200 (OK)
     - Successful
   * - 400 (BAD REQUEST)
     - If the resource uri is specified incorrectly.

Example
^^^^^^^^
.. code-block:: bash

 $ curl -H 'X-Loom-UserID:admin' 
        -H 'X-Loom-ApiKey:<apikey>'
        http://<loom-server>:<loom-port>/<version>/loom/clustertemplates

