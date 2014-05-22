Module-specific markup
======================

This document tests sphinx markup from section "Module-specific markup" in the sphinx docs, 
using the snippets from the manual.

The rst2pdf output should be a hybrid between the LaTeX and HTML outputs.

.. module:: parrot
   :platform: Unix, Windows
   :synopsis: Analyze and reanimate dead parrots.

.. function:: spam(eggs)
              ham(eggs)

   Spam or ham the foo.
   
.. cfunction:: PyObject* PyType_GenericAlloc(PyTypeObject *type, Py_ssize_t nitems)

.. cmember:: PyObject* PyTypeObject.tp_bases

.. cvar:: PyObject* PyClass_Type

.. function:: Timer.repeat([repeat=3[, number=1000000]])

.. class:: Foo

   .. method:: quux()

.. class:: Bar

.. method:: Bar.quux()

.. function:: compile(source[, filename[, symbol]])

.. function:: compile(source[, filename, symbol])

.. function:: compile(source : string[, filename, symbol]) -> ast object

.. function:: format_exception(etype, value, tb[, limit=None])

   Format the exception with a traceback.

   :param etype: exception type
   :param value: exception value
   :param tb: traceback object
   :param limit: maximum number of stack frames to show
   :type limit: integer or None
   :rtype: list of strings
   
   
.. cmdoption:: -m <module>, --module <module>

   Run a module as a script.
   
.. program:: rm

.. cmdoption:: -r

   Work recursively.

.. program:: svn

.. cmdoption:: -r revision

   Specify the revision to work upon.
   
:option:`rm -r` should refer to the first option, while :option:`svn -r` should refer to the second one.
   
.. describe:: opcode

   Describes a Python bytecode instruction.

