========
CDAP CLI
========

The CDAP CLI is a command-line interface for CDAP.

Adding New Commands
===================

Adding a new command can be done by adding your command to ``DefaultCommands``.
You can either add a single command by adding an instance of it to the first constructor argument
or by adding an instance of the command to one of the ``CommandSet`` in the second constructor argument.

Adding New Completers
=====================

Associating a completer with an ``ArgumentName`` can be done by registering your ``ArgumentName`` and completer
in ``DefaultCompleters``. Once your ``ArgumentName`` is associated with your completer, the CLI will autocomplete
your ``ArgumentName`` in command patterns for the users.

For example, if you register an ``ArgumentName`` named "app-jar-file" with a ``FileNameCompleter``, and
you have a command that has the pattern ``deploy app <app-jar-file>``, then the CLI will use the ``FileNameCompleter``
to autocomplete when the user requests autocompletion for ``<app-jar-file>``.
