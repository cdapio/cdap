CDAP Authentication Client for Python
=====================================

The Authentication Client Python API can be used for fetching the access
token from the CDAP authentication server to interact with a secure CDAP
cluster.

Supported Actions
-----------------

-  Check that authentication is enabled in the CDAP cluster.
-  Fetch an access token from the authentication server with credentials
   supported by the active authentication mechanism.

The default implementation of the authentication
client—\ ``BasicAuthenticationClient``—supports the default
authentication mechanisms supported by CDAP:

-  Basic Authentication
-  LDAP
-  JASPI

Custom Authentication Mechanism
-------------------------------

If CDAP is configured to use a custom authentication mechanism, a custom
authentication client is needed to fetch the access token. Custom
authentication clients must implement the ``AuthenticationClient``
interface. The ``AbstractAuthenticationClient`` class contains common
functionality required by authentication clients, and can be extended by
the custom authentication client.

Installation
------------

To install the latest release version of CDAP Authentication Client, run

::

    $ pip install cdap-auth-client

To install the development version, clone the repository

::

    $ git clone https://github.com/caskdata/cdap-clients.git
    $ cd cdap-clients/cdap-authentication-clients/python/
    $ python setup.py install

Supported Python versions: 2.6, 2.7

Usage
-----

To use the Authentication Client Python API, include this import in your
Python script:

::

    from cdap_auth_client import BasicAuthenticationClient

Example
-------

Create a BasicAuthenticationClient instance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    authentication_client = BasicAuthenticationClient()

Set the CDAP connection information
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-  hostname
-  port
-  boolean flag, ``True`` if SSL is enabled

Example:

::

    authentication_client.set_connection_info('localhost', 10000, False)

This method should be called only once for every
``AuthenticationClient`` object.

Check if authentication is enabled in the CDAP cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    is_enabled = authentication_client.is_auth_enabled()

Configure Authentication Client
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Set the required fields on a
`dictionary <https://docs.python.org/2/tutorial/datastructures.html#dictionaries>`__:

::

    properties = {
      'security_auth_client_username': 'admin',
      'security_auth_client_password': 'secret',
      'security_ssl_cert_check': True
    }

If authentication is enabled, configure the Authentication Client with
user credentials and other properties (this method should be called only
once for every ``AuthenticationClient`` object).

::

    authentication_client.configure(properties)

**Note:**

-  The ``BasicAuthenticationClient`` requires these user credentials:

   -  ``security_auth_client_username=username``
   -  ``security_auth_client_password=password``

-  When SSL is enabled, to suspend certificate checks and allow
   self-signed certificates, set
   ``security.security_ssl_cert_check=false``.
-  For non-interactive applications, user credentials will come from a
   configuration file.
-  For interactive applications, see the section `Interactive
   Applications <#interactive-applications>`__ below on retrieving and
   using user credentials.

Retrieve and use the access token for the user from the authentication server
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    token = authentication_client.get_access_token()

    headers = { 'Authorization': token.token_type + ' ' + token.value }
    requests.request(method, url, headers=headers)

If there is an error while fetching the access token, an ``IOError``
will be raised.

Interactive Applications
------------------------

This example illustrates obtaining user credentials in an interactive
application, and then configuring the Authentication Client with the
retrieved credentials.

::

    authentication_client.set_connection_info('localhost', 10000, False)
    properties = {}

    if authentication_client.is_auth_enabled():
      for credential in authentication_client.get_required_credentials():
         print('Please specify %s > ' % credential.get_description())
         if credential.is_secret():
            credential_value = getpass.getpass()
         else:
            credential_value = raw_input()
         properties[credential.get_name()] = credential_value
      authentication_client.configure(properties)

Tests
-----

To run tests from a command line:

``python tests/BasicAuthenticationClientTest.py``
