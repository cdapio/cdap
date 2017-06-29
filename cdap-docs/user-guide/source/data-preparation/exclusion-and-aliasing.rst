.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

===============================================
Excluding (Restricting) and Aliasing Directives
===============================================

When a organization wants to exposes data prep tool to it's end users,
they would like to have the ability to restrict (exclude) directives
that are not considered "safe". Safe is very subjective and varies from
organization to organization. Also, the "Safe"ness of an data operation
could go through approval process and later excluded from the restricted
list. So, in short, the capability to restrict and un-restrict has be
easily configurable.

Second common use-case we have seen is that an organization is
accustomed to a organizational jargon and it's hard to adapt, it's not
impossible, it just hard.

In order to support these kind of usage, Data Prep has added the
capability to exclude and as well as alias a directive with a simple
configuration.

Feature
-------

There are two configuration supported by Data Prep now

-  Exclusion (a.k.a Restriction) and
-  Aliasing

Exclusion allows administrators to specify a list of directives either
root directive or aliased directive that should be restricted from
invocation and as well as application.

Aliasing allows one to create a new name for a root directive.

Scope
-----

Both Exclusion and Aliasing are namespace wide - meaning they are
applicable only within the namespace were the configuration has been
applied.

Configuration
-------------

Configuration is currently specified as a JSON object with main keys
namely

-  ``exclusions``
-  ``aliases``

Following is a high-level JSON object

::

    {
        "exclusions" : [
          "root-directive",
          ...
          "root-directive"
        ],

        "aliases" : {
          "alias" : "alias-name",
          ...
          "alias" : "alias-name"
        }
    }

Exclusion
~~~~~~~~~

It's a array of directives that are either loaded by default or could be
loaded as UDD (User Defined Directives) or they can also be aliased
directives.

Aliases
-------

Is map of aliased directive and the actual directive name to which it's
aliased.

Applying Configuration
----------------------

A service endpoint exists to apply the configuration. In order to apply
the configuration, use following REST call.

::

    curl -s -X POST @<path-to-json>/<filename.json> \
     "http://<hostname>:11015/v3/namespaces/<namepsace>/apps/dataprep/services/service/methods/config"

And example would be

::

    curl -s -X POST --data-binary @/tmp/wrangler-config.json \
     http://localhost:11015/v3/namespaces/default/apps/dataprep/services/service/methods/config \
      | python -mjson.tool
    {
        "message": "Successfully updated configuration.",
        "status": 200
    }
