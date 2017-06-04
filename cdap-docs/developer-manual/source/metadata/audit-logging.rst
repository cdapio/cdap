.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016-2017 Cask Data, Inc.

.. _audit-logging:

=============
Audit Logging
=============

Overview
========
Audit logging provides a chronological ledger containing evidence of operations or changes
on CDAP entities. This information can be used to capture a trail of the activities that
determined the state of an entity at a given point in time. These activities include the
creation, modification, and deletion of an entity. It also includes modification of the
entity's :ref:`metadata <metadata>`. For data entities (datasets and
streams), it includes access information used to generate the entity's :ref:`lineage
<metadata-lineage>`. Audit logging is an especially important feature because it
enables users to integrate CDAP with external data governance systems such as
:ref:`Cloudera Navigator <navigator-integration>`.

Please note that audit logs are not published during a CDAP upgrade, as CDAP services are
not available. Hence, any application which uses CDAP audit logs to sync metadata will go 
out of sync with respect to changes made during the upgrade. Please see 
`CDAP-5954 <https://issues.cask.co/browse/CDAP-5954>`__ for details.

.. _audit-logging-supported-audit-events:

Supported Audit Events
======================
These audit events are supported in CDAP:

.. list-table::
   :widths: 30 40
   :header-rows: 1

   * - Audit Event Type
     - Supported Entities
   * - CREATE
     - * Datasets
       * Streams
   * - UPDATE
     - * Datasets
       * Streams
   * - DELETE
     - * Datasets
       * Streams
   * - TRUNCATE
     - * Datasets
       * Streams
   * - ACCESS
     - * Datasets
       * Streams
   * - METADATA_CHANGE
     - * Applications
       * Artifacts
       * Datasets
       * Programs
       * Streams

.. _audit-logging-configuring-audit-publishing:

Configuring Audit Publishing
============================
Audit publishing is controlled by these properties set in the ``cdap-site.xml``, as described in the
:ref:`Administration Manual <appendix-cdap-site.xml>`:

- ``audit.enabled``: Determines if publishing of audit logs is enabled; defaults to ``true``
- ``audit.topic``: Determines the topic to publish to; defaults to ``audit``

.. _audit-logging-consuming-audit-events:

Consuming Audit Events
======================
When audit publishing is :ref:`enabled <audit-logging-configuring-audit-publishing>`, for
:ref:`every audit event <audit-logging-supported-audit-events>`, a message is published to
:ref:`CDAP Kafka <admin-manual-cdap-components>` to the
:ref:`configured kafka topic <audit-logging-configuring-audit-publishing>`.

The contents of the message are a JSON representation of
the `AuditMessage
<https://github.com/caskdata/cdap/blob/develop/cdap-proto/src/main/java/co/cask/cdap/proto/audit/AuditMessage.java>`__
class.

Here are some example JSON messages, pretty-printed:

**Dataset Creation**

::

  {
	  "version": 1,
	  "time": 1000,
	  "entityId": {
		  "namespace": "ns1",
		  "dataset": "ds1",
		  "entity": "DATASET"
	  },
	  "user": "user1",
	  "type": "CREATE",
	  "payload": {}
  }

**Stream Access**

::

  {
	  "version": 1,
	  "time": 2000,
	  "entityId": {
		  "namespace": "ns1",
		  "stream": "stream1",
		  "entity": "STREAM"
	  },
	  "user": "user1",
	  "type": "ACCESS",
	  "payload": {
		  "accessType": "WRITE",
		  "accessor": {
			  "namespace": "ns1",
			  "application": "app1",
			  "type": "Flow",
			  "program": "flow1",
			  "run": "run1",
			  "entity": "PROGRAM_RUN"
		  }
	  }
  }

**Application Metadata Change**

::

  {
	  "version": 1,
	  "time": 3000,
	  "entityId": {
  		"namespace": "ns1",
	  	"application": "app1",
		  "entity": "APPLICATION"
	  },
	  "user": "user1",
	  "type": "METADATA_CHANGE",
	  "payload": {
		  "previous": {
  			"USER": {
	  			"properties": {
		  			"uk": "uv",
			  		"uk1": "uv2"
				  },
				  "tags": ["ut1", "ut2"]
			  },
			  "SYSTEM": {
				  "properties": {
					  "sk": "sv"
				  },
				  "tags": []
			  }
		  },
		  "additions": {
			  "SYSTEM": {
				  "properties": {
					  "sk": "sv"
				  },
				  "tags": ["t1", "t2"]
			  }
		  },
		  "deletions": {
			  "USER": {
				  "properties": {
					  "uk": "uv"
				  },
				  "tags": ["ut1"]
			  }
		  }
	  }
  }

CDAP also provides an `adapter class 
<https://github.com/caskdata/cdap/blob/develop/cdap-proto/src/main/java/co/cask/cdap/proto/codec/AuditMessageTypeAdapter.java>`__
to enable deserializing of the audit messages using the `GSON <https://github.com/google/gson>`__ library.
