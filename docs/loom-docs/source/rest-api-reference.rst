:orphan:

.. _rest-api-reference:

========
REST API Reference
========

.. _api-summary

API Summary
===========


+------------------------+------------+----------+----------+
| Header row, column 1   | Header 2   | Header 3 | Header 4 |
| (header rows optional) |            |          |          |
+========================+============+==========+==========+
| body row 1, column 1   | column 2   | column 3 | column 4 |
+------------------------+------------+----------+----------+
| body row 2             | Cells may span columns.          |
+------------------------+------------+---------------------+
| body row 3             | Cells may  | - Table cells       |
+------------------------+ span rows. | - contain           |
| body row 4             |            | - body elements.    |
+------------------------+------------+---------------------+

Path
GET
POST
PUT
DELETE
v1/loom/providers	list of providers	add a provider
v1/loom/providers/<provider-id>	specification for a specific provider	 	replace provider, or create if not exists	delete specific provider
v1/loom/hardwaretypes
list of hardware types	add a hardware type
v1/loom/hardwaretypes/<hardwaretype-id>	specification for a specific hardware type	 	replace hardware type, or create if not exists	delete specific hardware type
v1/loom/imagetypes	list of images	add an image
v1/loom/imagetypes/<imagetype-id>	specification for a specific image type	 	replace image type, or create if not exists	delete specific image type
v1/loom/services	list of services and their specifications	add a service
v1/loom/services/<service-id>	specification for a specific service	 	replace service, or create if not exists	delete specific service
v1/loom/clustertemplates	list of templates	add a template
v1/loom/clustertemplates/<template-id>	specification for a specific template	 	replace template, or create if not exists	delete specific template
v1/loom/clusters	list of cluster ids	add a cluster
v1/loom/clusters/<cluster-id>	specification for a specific cluster	 	replace cluster	delete specific cluster
v1/loom/clusters/<cluster-id>/services	list of services for the cluster	add a service to the cluster
v1/loom/clusters/<cluster-id>/services/<service-id>	 	 	 	delete service from cluster
v1/loom/clusters/<cluster-id>/status	status of each node and services on each node



.. _configuration-macros

Configuration Macros
====================

.. glossary::
    :sorted:

    annotations
        Annotations are a concept used internally by SQLAlchemy in order to store
        additional information along with :class:`.ClauseElement` objects.  A Python
        dictionary is associated with a copy of the object, which contains key/value
        pairs significant to various internal systems, mostly within the ORM::

            some_column = Column('some_column', Integer)
            some_column_annotated = some_column._annotate({"entity": User})

        The annotation system differs from the public dictionary :attr:`.Column.info`
        in that the above annotation operation creates a *copy* of the new :class:`.Column`,
        rather than considering all annotation values to be part of a single
        unit.  The ORM creates copies of expression objects in order to
        apply annotations that are specific to their context, such as to differentiate
        columns that should render themselves as relative to a joined-inheritance
        entity versus those which should render relative to their immediate parent
        table alone, as well as to differentiate columns within the "join condition"
        of a relationship where the column in some cases needs to be expressed
        in terms of one particular table alias or another, based on its position
        within the join expression.

