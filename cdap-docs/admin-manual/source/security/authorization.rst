.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _admin-authorization:

=============
Authorization
=============
Authorization allows users to enforce fine-grained access control on CDAP entities:
namespaces, artifacts, applications, programs, datasets, streams, and secure keys. All
operations on these entities |---| listing, viewing, creating, updating, managing,
deleting |---| are governed by authorization policies.

.. _security-enabling-authorization:

Enabling Authorization
======================
To enable authorization in :term:`Distributed CDAP <distributed cdap>`, add these
properties to :ref:`cdap-site.xml <appendix-cdap-default-security>`:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Value
   * - ``security.authorization.enabled``
     -  true
   * - ``security.authorization.extension.extension.jar.path``
     - Absolute path of the JAR file to be used as the authorization extension. This file
       must be present on the local file system of the CDAP Master. In an HA environment, it
       should be present on the local file system of all CDAP Master hosts.

Authorization in CDAP only takes effect once :ref:`perimeter security
<admin-perimeter-security>` is also enabled by setting ``security.enabled`` to ``true``.
Additionally, it is recommended that Kerberos be enabled on the cluster by setting
``kerberos.auth.enabled`` to ``true``.

These additional properties can also be optionally modified to configure authorization:

- ``security.authorization.admin.users``
- ``security.authorization.cache.enabled``
- ``security.authorization.cache.refresh.interval.secs``
- ``security.authorization.cache.ttl.secs``

Please refer to :ref:`cdap-defaults.xml <appendix-cdap-default-security>` for
documentation on these configuration settings.

Authorization in CDAP is implemented as :ref:`authorization extensions
<authorization-extensions>`. Apart from the above configuration settings, an extension may
require additional properties to be configured. Please see the documentation on
individual extensions for configuring properties specific to that extension:

- :ref:`Integrations: Apache Sentry <apache-sentry>`

:ref:`Security extension properties <appendix-cdap-default-security>`, which are specified
in ``cdap-site.xml``, begin with the prefix ``security.authorization.extension.config``.

.. _security-cdap-instance:

CDAP Instances
==============
The concept of a *CDAP Instance* has been introduced to support authorization policies in
CDAP. A *CDAP Instance* is uniquely identified by an *instance name*, specified by the
property ``instance.name`` in the :ref:`cdap-site.xml <appendix-cdap-site.xml>`. Certain
operations in CDAP (listed in the next section) require privileges on the CDAP instance
identified by this property. One purpose of introducing this concept is to support, in the
future, the notion of multiple CDAP instances on which authorization policies are enforced
using the same authorization backend.

.. _security-authorization-policies:

Authorization Policies
======================
Currently, CDAP allows users to enforce authorization for *READ*, *WRITE*, *ADMIN*, and
*EXECUTE* operations. (This list will be expanded to allow for additional distinctions and
be made finer-grained in the future.)

In general, this summarizes the authorization policies in CDAP:

- A **create** operation on an entity requires *WRITE* on the entity's parent. For
  example, creating a namespace requires *WRITE* on the CDAP instance. Deploying an
  application, artifact, or dataset, or creating a stream requires a *WRITE* on the
  namespace.
- A **read** operation (such as reading from a dataset or a stream) on an entity requires
  *READ* on the entity.
- A **write** operation (such as writing to a dataset or a stream) on an entity requires
  *WRITE* on the entity.
- An **admin** operation (such as setting properties) on an entity requires *ADMIN* on
  the entity.
- A **delete** operation on an entity requires *ADMIN* on the entity.
- A **list** operation (such as listing or searching applications, datasets, streams,
  artifacts) only returns those entities that the logged-in user has at least one (*READ*,
  *WRITE*, *ADMIN*, *EXECUTE*) privilege on.
- A **view** operation on an entity only succeeds if the user has at least one (*READ*,
  *WRITE*, *ADMIN*, *EXECUTE*) privilege on it.

Additionally:

- Upon successful creation of an entity, the logged-in user receives all privileges
  (*READ*, *WRITE*, *ADMIN*, and *EXECUTE*) on that entity.
- Upon successful deletion of an entity, all privileges on that entity for all users are revoked.

CDAP supports hierarchical authorization enforcement, which means that an operation that
requires a certain privilege on an entity is allowed if the user has the same privilege on
the entity's parent. For example, reading from a CDAP dataset will succeed even if the
user does not have specific *READ* privileges on the dataset, but instead has *READ*
privileges on the namespace in which the dataset exists.

Authorization policies for various CDAP operations are listed in these tables:

.. _security-authorization-policies-namespaces:

Namespaces
----------

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Operation
     - Privileges Required
   * - Create
     - *WRITE* (on the CDAP instance)
   * - Update
     - *ADMIN*
   * - Delete
     - *ADMIN*
   * - List
     - Only returns those namespaces on which user has at least one of *READ, WRITE, ADMIN,* or *EXECUTE*
   * - View
     - At least one of *READ, WRITE, ADMIN,* or *EXECUTE*

.. _security-authorization-policies-artifacts:

Artifacts
---------

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Operation
     - Privileges Required
   * - Add
     - *WRITE* (on the namespace)
   * - Add a property
     - *ADMIN*
   * - Remove a property
     - *ADMIN*
   * - Delete
     - *ADMIN*
   * - List
     - Only returns those artifacts on which user has at least one of *READ*, *WRITE*, *ADMIN*, or *EXECUTE*
   * - View
     - At least one of *READ*, *WRITE*, *ADMIN*, or *EXECUTE*

.. _security-authorization-policies-applications:

Applications
------------

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Operation
     - Privileges Required
   * - Add
     - *WRITE* (on the namespace)
   * - Delete
     - *ADMIN*
   * - List
     - Only returns those applications on which user has at least one of *READ*, *WRITE*, *ADMIN*, or *EXECUTE*
   * - View
     - At least one of *READ*, *WRITE*, *ADMIN*, or *EXECUTE*

.. _security-authorization-policies-programs:

Programs
--------

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Operation
     - Privileges Required
   * - Start, Stop, or Debug
     - *EXECUTE*
   * - Set instances
     - *ADMIN*
   * - Set runtime arguments
     - *ADMIN*
   * - Retrieve runtime arguments
     - *READ*
   * - Retrieve status
     - At least one of *READ*, *WRITE*, *ADMIN*, or *EXECUTE*
   * - List
     - Only returns those programs on which user has at least one of *READ*, *WRITE*, *ADMIN*, or *EXECUTE*
   * - View
     - At least one of *READ*, *WRITE*, *ADMIN*, or *EXECUTE*

.. _security-authorization-policies-datasets:

Datasets
--------

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Operation
     - Privileges Required
   * - Create
     - *WRITE* (on the namespace)
   * - Read
     - *READ*
   * - Write
     - *WRITE*
   * - Update
     - *ADMIN*
   * - Upgrade
     - *ADMIN*
   * - Truncate
     - *ADMIN*
   * - Drop
     - *ADMIN*
   * - List
     - Only returns those artifacts on which user has at least one of *READ*, *WRITE*, *ADMIN*, or *EXECUTE*
   * - View
     - At least one of *READ*, *WRITE*, *ADMIN*, or *EXECUTE*

.. _security-authorization-policies-dataset-modules:

Dataset Modules
---------------

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Operation
     - Privileges Required
   * - Deploy
     - *WRITE* (on the namespace)
   * - Delete
     - *ADMIN*
   * - Delete-all in the namespace
     - *ADMIN* (on the namespace)
   * - List
     - Only returns those artifacts on which user has at least one of *READ*, *WRITE*, *ADMIN*, or *EXECUTE*
   * - View
     - At least one of *READ*, *WRITE*, *ADMIN*, or *EXECUTE*

.. _security-authorization-policies-dataset-types:

Dataset Types
-------------

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Operation
     - Privileges Required
   * - List
     - Only returns those artifacts on which user has at least one of *READ*, *WRITE*, *ADMIN*, or *EXECUTE*
   * - View
     - At least one of *READ*, *WRITE*, *ADMIN*, or *EXECUTE*

.. _security-authorization-policies-secure-keys:

Secure Keys
-----------

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Operation
     - Privileges Required
   * - Create
     - *WRITE* (on the namespace)
   * - Delete
     - *ADMIN*
   * - List
     - Only returns those artifacts on which user has at least one of *READ*, *WRITE*, *ADMIN*, or *EXECUTE*
   * - View
     - At least one of *READ*, *WRITE*, *ADMIN*, or *EXECUTE*

.. _security-authorization-policies-streams:

Streams
-------

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Operation
     - Privileges Required
   * - Create
     - *WRITE* (on the namespace)
   * - Retrieving events
     - *READ*
   * - Retrieving properties
     - At least one of *READ*, *WRITE*, *ADMIN*, or *EXECUTE*
   * - Sending events to a stream (sync, async, or batch)
     - *WRITE*
   * - Drop
     - *ADMIN*
   * - Drop-all in the namespace
     - *ADMIN* (on the namespace)
   * - Update
     - *ADMIN*
   * - Truncate
     - *ADMIN*
   * - List
     - Only returns those artifacts on which user has at least one of *READ*, *WRITE*, *ADMIN*, or *EXECUTE*
   * - View
     - At least one of *READ*, *WRITE*, *ADMIN*, or *EXECUTE*


.. _security-bootstrapping-authorization:

Bootstrapping Authorization
===========================
When CDAP is first started with authorization enabled, no users are granted privileges on
any CDAP entities. Without any privileges, it can be impossible to bootstrap CDAP (create
a new namespace or create entities in the *default* namespace) unless an
external interface (such as `Hue <http://gethue.com/>`__) is used for a supported
authorization extension (such as :ref:`Integrations: Apache Sentry <apache-sentry>`).

To make this bootstrap process easier, during startup the CDAP Master issues these grants to
select users:

- The user that runs the CDAP Master is granted *ADMIN* on the CDAP instance, so that the
  *default* namespace can be created by that user if it does not already exist.

- The user that runs the CDAP Master is granted *READ*, *WRITE*, *ADMIN*, and
  *EXECUTE* on the system namespace, so operations such as creation of system tables,
  deployment of system artifacts, and deployment of system dataset modules can be
  performed by that user.

- Additionally, a comma-separated list of users specified as the
  ``security.authorization.admin.users`` in ``cdap-site.xml`` is granted *ADMIN*
  privileges on the CDAP instance and the *default* namespace, so that they have the
  required privileges to create namespaces and grant other users access to the *default*
  namespace. It is recommended that this property be set to the list of users that will
  administer and manage the CDAP installation.


.. _security-auth-policy-pushdown:

Authorization Policy Pushdown
=============================
Currently, CDAP does not support the pushing of authorization policy grants and revokes to
:term:`storage providers <storage provider>`. As a result, when a user is granted *READ*
or *WRITE* access on existing datasets or streams, permissions are not updated in the
storage providers. The same applies when authorization policies are revoked.

A newly-applied authorization policy will be enforced when the dataset or stream is
accessed from CDAP, but not when it is accessed directly in the storage provider. If the
pushdown of permissions to storage providers is desired, it needs to be done manually.
This will be done automatically in a future release of CDAP.

This limitation has a larger implication when :ref:`Cross-namespace Dataset Access
<cross-namespace-dataset-access>` is used. When accessing a dataset from a different
namespace, CDAP currently presumes that the user accessing the dataset has been granted
permissions on the dataset in the storage provider prior to accessing the dataset from
CDAP. 

For example, if a program in the namespace *ns1* tries to access a :term:`fileset` in the
namespace *ns2*, the user running the program should be granted the appropriate (*READ*,
*WRITE*, or both) privileges on the fileset. Additionally, the user needs to be granted
appropriate permissions on the HDFS directory that the fileset points to. When
:ref:`impersonation <admin-impersonation>` is used in the program's namespace, this user
is the impersonated user, otherwise it is the user that the CDAP Master runs as.
