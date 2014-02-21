.. _guide_installation_toplevel:

.. index::
   single: Openstack Configuration
=======================
Openstack Configuration
=======================

* Admin:
 * There must be a user in OpenStack which is a member of all projects which will have hosts provisioned by Loom.
* Networking:
 * Instance networks must be routable from Loom provisioners.
 * If multiple networks, any network named "public" becomes network Loom Provisioner uses for SSH.
 * If there is only one network, loom provisioner will use it for SSH.
 * Loom currently does not support specifying a network.
* SSH Keys:
 * OpenStack must be configured with ``libvirt_inject_key true`` for key-based authentication to instances.
 * The key must be present in OpenStack and ``openstack_ssh_key_id`` must be configured in the provider.
 * The private key file must be present on the Loom provisioner machines in the path specified in ``identity_file`` for the provider.
 * The private key file must be used when using SSH to connect to the instance, rather than a password.
* Passwords:
 * OpenStack must be configured with ``libvirt_inject_password true`` for password-based authentication to instances.
* Operating Systems
 * Linux instances are currently supported.
