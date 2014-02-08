:orphan:

.. _faq_toplevel:
.. include:: /toplevel-links.rst

============================
Loom Administration
============================

What operations are only available to the admin versus other users?
-------------------------------------------------------------------

Administrators are able to see all clusters, whereas users are only
able to see clusters they own.  Admins are also the only ones able
to add and edit providers, image types, hardware types, services, 
and cluster templates.

What happens to existing clusters when the template used to create them changes?
--------------------------------------------------------------------------------

Existing clusters are not affected by any changes to providers, image types, 
hardware types, services, or cluster templates that are made after the time of
cluster creation. Each cluster keeps a copy of the template state at the time of
creation.

Note that clusters created with the old template will always retain their old configurations. 
Ability to update the clusters with the modifed template is not presently supported.

How can I write configuration settings that reference hostnames of other nodes in the cluster?
----------------------------------------------------------------------------------------------

Loom provides macros that allow you to do this.  See :doc:`Macros</guide/admin/macros>` for more information. 


Is there any way to limit how many nodes or clusters a user can provision?
--------------------------------------------------------------------------

No, there is no way to do this at the moment, but there are plans to add support for this in the future.

Can I configure clusters to delete themselves after some amount of time?
------------------------------------------------------------------------

No, there is no way to do this at the moment, but there are plans to add lease times in the future.
