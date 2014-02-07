:orphan:

.. _faq_toplevel:
.. include:: /toplevel-links.rst

====================================
Security
====================================

Does Loom support authentication ?
----------------------------------
Loom backend has minimal support for authentication. In the next version, there will 
be integration with Crowd and LDAP servers allowing users to authenticate against the 
directories available.

Are all the communication between Loom Server and Loom Provisioners secure
------------------------------------------------------------------------------------
Not right now, but the plan is to move them to communicate on https during future releases. 
This is not an immediate concern as there is no user sensitive data being passed around between 
them.

Can Loom integrate with any authentication system ?
---------------------------------------------------
It's designed to integrate with any authentication system, next release will include support
for Crowd and LDAP and later releases will open it up to integate with different systems.

Will Loom support authorization and granular control in future ?
-----------------------------------------------------------------
Absolutely, this feature is few releases down the lane, but it's one of the most important features that Loom 
will be supporting.
