CDAP Authentication Client for Java
===================================

The Authentication Client API can be used for fetching the access token
from the CDAP authentication server to interact with a secure CDAP
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
authentication client will have to be written to fetch the access token.
The custom authentication clients needs to implement the
``AuthenticationClient`` interface. The ``AbstractAuthenticationClient``
class contains common functionality required by authentication clients,
and can be extended by the custom authentication client. The custom
authentication client has to be placed into the classpath of the
application that needs to use it.

Build
-----

To build the Authentication Client API jar, use:

::

    mvn clean package

Usage
-----

To use the Authentication Client API, include this Maven dependency in
your project's ``pom.xml`` file:

::

    <dependency>
      <groupId>co.cask.cdap</groupId>
      <artifactId>cdap-authentication-client</artifactId>
      <version>1.1.0-SNAPSHOT</version>
    </dependency>

Example
-------

Create an Authentication Client instance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    String defaultAuthClientClass =
      "co.cask.cdap.security.authentication.client.basic.BasicAuthenticationClient";
    String authClientClassName =
       properties.getProperty("security.auth.client.class.name", defaultAuthClientClass);
    AuthenticationClient authenticationClient =
       (AuthenticationClient) Class.forName(authClientClassName).newInstance();

Creating an Authentication Client using a configuration parameter as
shown above allows plugging in custom implementations of Authentication
Client.

Set the CDAP connection information
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-  hostname
-  port
-  boolean flag, true if SSL is enabled

Example:

::

    authenticationClient.setConnectionInfo("localhost", 10000, false);

This method should be called only once for every
``AuthenticationClient`` object.

Check if authentication is enabled in the CDAP cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    boolean isEnabled = authenticationClient.isAuthEnabled();

Configure Authentication Client
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If authentication is enabled, configure the Authentication Client with
user credentials and other properties (this method should be called only
once for every ``AuthenticationClient`` object).

::

    authenticationClient.configure(properties);

**Note:**

-  The ``BasicAuthenticationClient`` requires these user credentials:

   -  ``security.auth.client.username=username``
   -  ``security.auth.client.password=password``

-  When SSL is enabled, to suspend certificate checks and allow
   self-signed certificates, set
   ``security.auth.client.verify.ssl.cert=false``.
-  For non-interactive applications, user credentials will come from a
   configuration file.
-  For interactive applications, see the section `Interactive
   Applications <#interactive-applications>`__ below on retrieving and
   using user credentials.

Retrieve the access token for the user from the authentication server, and use it
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    HttpURLConnection conn = (HttpURLConnection) cdapURL.openConnection();
    conn.setRequestProperty("Authorization", 
               authenticationClient.getAccessToken().getTokenType() + " " +
               authenticationClient.getAccessToken().getValue());
    // ...
    conn.connect();

If there is an error while fetching the access token, an ``IOException``
will be thrown. The Authentication Client caches the access token until
the token expires. It automatically re-fetches a new token upon expiry.

Interactive Applications
------------------------

This example illustrates obtaining user credentials in an interactive
application, and then configuring the Authentication Client with the
retrieved credentials.

::

    authenticationClient.setConnectionInfo(hostname, port, ssl);
    Properties properties = new Properties();

    if (authenticationClient.isAuthEnabled()) {
      ConsoleReader reader = new ConsoleReader();
      for (Credential credential : authenticationClient.getRequiredCredentials()) {
        String credentialValue;
        output.printf("Please, specify "  credential.getDescription()  "> ");
        if (credential.isSecret()) {
            credentialValue = reader.readLine(prompt, '*');
        } else {
          credentialValue = reader.readLine(prompt);
        }
        properties.put(credential.getName(), credentialValue);
      }

      authenticationClient.configure(properties);
    }

