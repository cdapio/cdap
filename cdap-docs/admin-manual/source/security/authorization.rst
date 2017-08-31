.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016-2017 Cask Data, Inc.

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
   * - ``security.authorization.extension.jar.path``
     - Absolute path of the JAR file to be used as the authorization extension. This file
       must be present on the local file system of the CDAP Master. In an HA environment, it
       should be present on the local file system of all CDAP Master hosts.
   * - ``security.authorization.extension.extra.classpath`` (Optional)
     - Extra classpath for security extension

Authorization in CDAP only takes effect once :ref:`perimeter security
<admin-perimeter-security>` is also enabled by setting ``security.enabled`` to ``true``.
Additionally, Kerberos must be enabled on the cluster and for CDAP by setting
``kerberos.auth.enabled`` to ``true`` since CDAP Authorization depends on Kerberos.

These additional properties can also be optionally modified to configure authorization:

- ``security.authorization.cache.max.entries``
- ``security.authorization.cache.ttl.secs``

Please refer to :ref:`cdap-defaults.xml <appendix-cdap-default-security>` for
documentation on these configuration settings.

Authorization in CDAP is implemented as :ref:`authorization extensions
<authorization-extensions>`. Apart from the above configuration settings, an extension may
require additional properties to be configured. Please see the documentation on
individual extensions for configuring properties specific to that extension:

- :ref:`Integrations: Apache Sentry <apache-sentry>`
- `Integrations: Apache Ranger <https://github.com/caskdata/cdap-security-extn/wiki/CDAP-Ranger-Extension>`_

:ref:`Security extension properties <appendix-cdap-default-security>`, which are specified
in ``cdap-site.xml``, begin with the prefix ``security.authorization.extension.config``.


.. _security-authorization-policies:

Authorization Policies
======================
Currently, CDAP allows users to enforce authorization for *READ*, *WRITE*, *EXECUTE*, and
*ADMIN* operations. The authorization model requires pre-granted privileges on entities for all
operations.

In general, this summarizes the authorization policies in CDAP:

- A **create** operation on an entity requires *ADMIN* on the entity. Privileges need to be pre-granted to
  create the entity. For example, creating a namespace requires *ADMIN* on the namespace itself.
- A **read** operation (such as reading from a dataset or a stream) on an entity requires
  *READ* on the entity.
- A **write** operation (such as writing to a dataset or a stream) on an entity requires
  *WRITE* on the entity.
- An **admin** operation (such as setting properties) on an entity requires *ADMIN* on
  the entity.
- A **delete** operation on an entity requires *ADMIN* on the entity. Note that if the deletion operation will delete
  multiple entities, *ADMIN* is required on all the entities. For example, delete on a namespace requires *ADMIN* on
  all entities in the namespace.
- A **execute** operation on a program requires *EXECUTE* on the program.
- A **list** or **view** operation (such as listing or searching applications, datasets, streams,
  artifacts) only returns those entities that the logged-in user has at least one (*READ*,
  *WRITE*, *EXECUTE*, *ADMIN*) privilege on or on any of its descendants.
- A **get** operation on an entity (such as getting the dataset property, app detail) only succeeds if the user has
  at least one (*READ*, *WRITE*, *EXECUTE*, *ADMIN*) privilege on it or any of its descendants.
- Only admins of the authorization backend can grant or revoke the privileges.

Additionally:

- Upon successful creation/deletion of an entity, the privileges remain unaffected.
  It is the responsibility of the administrator to delete privileges from the authorization backend on entity deletion.
  If the privileges are not deleted and the entity is recreated the privileges will affect the enforcement on the entity.
- More privileges are needed for the following operations in the following cases:
    - Deploying an application with new artifact requires *ADMIN* on the artifact.
    - Deploying an application with existing artifact requires *READ, WRITE, EXECUTE,* or *ADMIN* on the artifact.
    - Deploying an application requires *ADMIN* privilege on all the datasets and streams that will be created
      along with the app, and *ADMIN* privilege on the dataset module and types if there are custom datasets.
    - Creating a dataset needs *ADMIN* privilege on the dataset module and types if it is a custom dataset.
    - If no impersonation is involved, correct privileges on the streams and datasets needs to be given to *cdap* to allow
      *cdap* accessing these entities.
    - If impersonation is involved, *admin* privilege on the principal is required to create a namespace, deploy an app
      create dataset or streams.

CDAP does **not** support hierarchical authorization enforcement, which means that privileges on each entity
are evaluated independently. CDAP has the concept of visibility, which means user will be able to view the entity if
the user has any of the privilege on the entity or any of its descendants. For example, user can see the application if the user has
*ADMIN* on the program. Note that visibility should not be confused with enforcement. In this case the user will not be
able to to perform *ADMIN* action on the application itself.

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
     - *ADMIN*
   * - Update
     - *ADMIN*
   * - Delete
     - *ADMIN* on the namespace, and *ADMIN* on all entities in the namespace, note that lack of the privileges may
       result in an inconsistent state for the namespace. Some entities may get cleaned up while entities with insufficient
       privileges will remain.
   * - List/View
     - Only returns those namespaces which user has at least one of *READ, WRITE, EXECUTE,* or *ADMIN* on the
       namespace or on any of its descendants
   * - Get
     - At least one of *READ, WRITE, EXECUTE,* or *ADMIN* on the namespace or any of its descendants

.. _security-authorization-policies-artifacts:

Artifacts
---------

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Operation
     - Privileges Required
   * - Add
     - *ADMIN*
   * - Add a property
     - *ADMIN*
   * - Remove a property
     - *ADMIN*
   * - Delete
     - *ADMIN*
   * - List/View
     - Only returns those artifacts on which user has at least one of *READ, WRITE, EXECUTE,* or *ADMIN*
   * - Get
     - At least one of *READ, WRITE, EXECUTE,* or *ADMIN*

.. _security-authorization-policies-applications:

Applications
------------

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Operation
     - Privileges Required
   * - Add
     - *ADMIN* (on the application) and *ADMIN* (if adding new artifacts) or
       any privileges(if using existing artifacts) on the artifact
   * - Delete
     - *ADMIN*
   * - List/View
     - Only returns those applications which user has at least one of *READ, WRITE, EXECUTE,* or *ADMIN* on the
       application or on any of its descendants
   * - Get
     - At least one of *READ, WRITE, EXECUTE,* or *ADMIN* on the application or any of its descendants

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
     - At least one of *READ, EXECUTE* or *ADMIN*
   * - Retrieve status
     - At least one of *READ, WRITE, EXECUTE,* or *ADMIN*
   * - List/View
     - Only returns those programs on which user has at least one of *READ, WRITE, EXECUTE,* or *ADMIN*
   * - Get
     - At least one of *READ, WRITE, EXECUTE,* or *ADMIN*
   * - Resume/Suspend schedule
     - *EXECUTE*

.. _security-authorization-policies-datasets:

Datasets
--------

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Operation
     - Privileges Required
   * - Create
     - *ADMIN* on the dataset and, for custom datasets, at least one of *READ, WRITE, EXECUTE,* or *ADMIN* on the
       dataset type
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
   * - List/View
     - Only returns those datasets on which user has at least one of *READ, WRITE, EXECUTE,* or *ADMIN*
   * - Get
     - At least one of *READ, WRITE, EXECUTE,* or *ADMIN*

.. _security-authorization-policies-dataset-modules:

Dataset Modules
---------------

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Operation
     - Privileges Required
   * - Deploy
     - *ADMIN*
   * - Delete
     - *ADMIN*
   * - Delete-all in the namespace
     - *ADMIN* on all dataset modules in the namespace
   * - List/View
     - Only returns those dataset modules on which user has at least one of *READ, WRITE, EXECUTE,* or *ADMIN*
   * - Get
     - At least one of *READ, WRITE, EXECUTE,* or *ADMIN*

.. _security-authorization-policies-dataset-types:

Dataset Types
-------------

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Operation
     - Privileges Required
   * - List/View
     - Only returns those dataset types on which user has at least one of *READ, WRITE, EXECUTE,* or *ADMIN*
   * - Get
     - At least one of *READ, WRITE, EXECUTE,* or *ADMIN*

.. _security-authorization-policies-secure-keys:

Secure Keys
-----------

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Operation
     - Privileges Required
   * - Create
     - *ADMIN*
   * - READ the secure data
     - *READ*
   * - Delete
     - *ADMIN*
   * - List/View
     - Only returns those secure keys on which user has at least one of *READ, WRITE, EXECUTE,* or *ADMIN*

.. _security-authorization-policies-streams:

Streams
-------

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Operation
     - Privileges Required
   * - Create
     - *ADMIN*
   * - Retrieving events
     - *READ*
   * - Sending events to a stream (sync, async, or batch)
     - *WRITE*
   * - Drop
     - *ADMIN*
   * - Drop-all in the namespace
     - *ADMIN* on all streams in the namespace
   * - Update
     - *ADMIN*
   * - Truncate
     - *ADMIN*
   * - List/View
     - Only returns those streams on which user has at least one of *READ, WRITE, EXECUTE,* or *ADMIN*
   * - Get
     - At least one of *READ, WRITE, EXECUTE,* or *ADMIN*

.. _security-authorization-policies-principal:

Kerberos Principal
------------------

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Operation
     - Privileges Required
   * - Deploy an app to impersonate a kerberos principal
     - *ADMIN* on the principal
   * - Create a namespace with owner prinicpal
     - *ADMIN* on the principal
   * - Create a dataset with owner prinicpal
     - *ADMIN* on the principal
   * - Create a stream with owner prinicpal
     - *ADMIN* on the principal


.. _security-pre-grant-wildcard-privilege:

Pre-grant and Wildcard Privileges
=================================
The new authorization model requires pre-granted privilege on all entity for any operation.
When CDAP is first started with authorization enabled, no users are granted privileges on
any CDAP entities. Without any privileges, CDAP will not be able to create the default namespace.
To create the default namespace, grant *ADMIN* on default namespace to the CDAP master user.
The default namespace will get created in several minutes automatically.

To pre-grant the privilege, wildcard can be used to minimize the burden of granting privileges on all entities.
Detailed ways of granting privileges can be found in the following sections for different authorization backends.

.. _security-sentry-integration:

Sentry Integration
------------------
:ref:`CDAP CLI <cdap-cli>` can be used to grant or revoke the privileges for :ref:`Integrations: Apache Sentry <apache-sentry>`.

You can use the :ref:`CDAP CLI <cdap-cli>` to issue :ref:`security commands <cli-available-commands-security>`.
Wildcard can be used to grant or revoke actions on multiple entities by including ``*`` and ``?`` in the entity name:

- To grant a principal privileges to perform certain actions on an entity, use::

    > grant actions <actions> on entity <entity-id> to <principal-type> <principal-name>
    > revoke actions <actions> on entity <entity-id> from <principal-type> <principal-name>

  where:

  - ``<actions>`` is a comma-separated list of privileges, any of *READ, WRITE, EXECUTE,* or *ADMIN*.

  - ``<entity>`` is of the form ``<entity-type>:<entity-id>``, where ``<entity-type>`` is
    one of ``namespace``, ``artifact``, ``application``, ``dataset``, ``program``, ``stream``, ``dataset_type`` or
    ``dataset_module``.

  - For namespaces, ``<entity-id>`` is composed from the namespace, such as
    ``namespace:<namespace-name>``.

  - For datasets, streams, artifacts and apps, ``<entity-id>`` is the namespace and entity names, such as
    ``<namespace-name>.<dataset-name>``, ``<namespace-name>.<stream-name>``, ``<namespace-name>.<artifact-name>``,
    and ``<namespace-name>.<app-name>``.

  - For programs, ``<entity-id>`` includes the application name and the program type:
    ``<namespace-name>.<app-name>.<program-type>.<program-name>``. ``<program-type>`` is
    one of flow, mapreduce, service, spark, worker, or workflow.

  - For datasets, streams, artifacts and apps, ``<entity-id>`` is the namespace and entity names, such as
    ``<namespace-name>.<dataset-name>``, ``<namespace-name>.<stream-name>``, ``<namespace-name>.<artifact-name>``,
    and ``<namespace-name>.<app-name>``.

  - ``<principal-type>`` can **only** be ``role`` since Sentry only supports granting privileges to roles.

  - Wildcard can be used in each entity name to grant privileges to multiple entities. For example,
    ``namespace:ns*`` represents all namespaces that starts with ``ns``.
    ``namespace:ns?`` represents all namespaces that starts with ``ns`` and follows by a single character.
    ``program:ns1.app1.*`` represents all types of programs in application app1 in namespace ns1.

- To add the role to other principal, use::

    > add role <role-name> to <principal-type> <principal-name>

  where:

  - ``<role-name>`` is the role name that adds to the principal.

  - ``<principal-type>`` can **only** be ``group``.

- To create a new role, use::

    > create role <role-name>

- To check the results, list the privileges for a principal::

    > list privileges for <principal-type> <principal-name>

For example,
to make ``alice``, in group ``admin``, as the administrator on a namespace ``ns1`` in a new environment,
do the following steps:

- create a new role ``ns1_administrator``

- use the commands to grant *ADMIN* on these entities: ``namespace:ns1``, ``application:ns1.*``, ``program:ns1.*.*``,
  ``artifact:ns1.*``, ``dataset:ns1.*``, ``stream: ns1.*``, ``dataset_type:ns1.*``, ``dataset_module:ns1.*``,
  ``securekey:ns1.*`` and ``kerberosprincipal.*`` to the role ``ns1_administrator``

- add ``ns1_administrator`` to group ``admin``

Note that:

- Only users in sentry admin group can be used to grant/revoke the privileges, this property can be set or updated by
  changing property ``sentry.service.admin.group`` in sentry.
- Any update to privileges will take some time to take effect based on the cache timeout. By default, the maximum
  time will be 10 minutes.

.. _security-ranger-integration:

Ranger Integration
------------------
To use Apache Ranger as the authorization backend, please refer to `CDAP Ranger Extension <https://github.com/caskdata/cdap-security-extn/wiki/CDAP-Ranger-Extension>`_


.. _security-differences-between-new-and-old-model:

Differences Between New and Old Model
=====================================
CDAP has migrated to the new auth model in 4.3 and old auth model will not work. The detailed new authorization policy
can be checked :ref:`above <security-authorization-policies>`.

In general, this summarizes the authorization policies change in CDAP:
   - No hierarchical authorization enforcement is supported, which means having a privilege on an entity's parent does
     not give that privilege on the entity. For example, having *READ* on the namespace does not give *READ* to
     the datasets and streams in the namespace.
   - No authorization bootstrap, no privileges on instance and no admin users. The new model removes the requirement
     of privileges on CDAP instance and admin users. Each privilege needs to be pre-granted to create the entity
     either through CDAP CLI or through an external interface of the supported authorization extension.
   - Automatic grant on entity creation and automatic revoke on entity deletion are removed. It is the responsibility
     of the administrator to create and delete privileges.


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
