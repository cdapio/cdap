:orphan:

.. _faq_toplevel:

.. index::
   single: FAQ: Loom Administration
============================
Loom Administration
============================

What operations are only available to the admin versus other users?
-------------------------------------------------------------------

Administrators can access all clusters, whereas users can access only
clusters they own. Only Administrators have super-user operarations such as adding, deleting, and editing providers,
images, hardware types, services, and cluster templates.

What happens to existing clusters when the template used to create them changes?
--------------------------------------------------------------------------------

Existing clusters are not affected by any changes to providers, image types, 
hardware types, services, or cluster templates that are made after the time of
cluster creation. Each cluster keeps a copy of the template state at the time of
creation.

Note that clusters created with the original template will always retain their old configurations. 
Ability to update the clusters with the modifed template is not currently supported.

How can I write configuration settings that reference hostnames of other nodes in the cluster?
----------------------------------------------------------------------------------------------

Loom provides a set of macros that allow you to do this.  See :doc:`Macros</guide/admin/macros>` for more information. 


Can I configure clusters to delete themselves after some amount of time?
------------------------------------------------------------------------

Not at this moment, but there are plans to add lease times in the next release.
