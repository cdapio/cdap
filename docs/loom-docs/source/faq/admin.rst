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
cluster creation.  Each cluster keeps a copy of the template state at the time of
creation.
