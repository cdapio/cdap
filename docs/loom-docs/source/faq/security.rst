:orphan:

.. _faq_toplevel:

.. index::
   single: FAQ: Security
====================================
Security
====================================

Does Loom support authentication?
----------------------------------
Loom backend has minimal support for authentication. In the next version, there will 
be integration with Crowd and LDAP servers allowing users to authenticate against the 
available directories.

Are all communications between Loom Server and Loom Provisioners secure?
------------------------------------------------------------------------------------
Not right now, but the plan is to move them to communicate on https in future releases. 
This is not an immediate concern, since there is no user sensitive data passed between 
them.

Can Loom integrate with any authentication system?
---------------------------------------------------
It's designed to integrate with any authentication system. The next release will include support
for OpenID, LDAP and OAuth, and the later releases will open up integration with different systems.

Will Loom support authorization and granular control in future?
-----------------------------------------------------------------
Absolutely, this feature is few releases down the lane. Indeed, it's one of the most important features that Loom 
will be supporting. In large scale deployment of clusters and nodes, having granular and role based access control are 
imperative for auditing and accountability.
