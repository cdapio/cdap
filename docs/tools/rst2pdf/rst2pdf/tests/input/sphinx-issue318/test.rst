.. Issue 318 documentation master file, created by
   sphinx-quickstart on Sun Oct  3 18:23:18 2010.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Issue 318's documentation!
=====================================

Contents:

.. toctree::
   :maxdepth: 2


.. cpp:function:: bool namespaced::theclass::method(int arg1, std::string arg2)

   Describes a method with parameters and types.

.. cpp:function:: bool namespaced::theclass::method(arg1, arg2)

   Describes a method without types.

.. cpp:function:: const T &array<T>::operator[]() const

   Describes the constant indexing operator of a templated array.

.. cpp:function:: operator bool() const

   Describe a casting operator here.

.. cpp:member:: std::string theclass::name

.. cpp:type:: theclass::const_iterator

.. py:function:: format_exception(etype, value, tb[, limit=None])

   Format the exception with a traceback.

   :param etype: exception type
   :param value: exception value
   :param tb: traceback object
   :param limit: maximum number of stack frames to show
   :type limit: integer or None
   :rtype: list of strings

.. module:: parrot
   :platform: Unix, Windows
   :synopsis: Analyze and reanimate dead parrots.

.. function:: spam(eggs)
              ham(eggs)

   Spam or ham the foo.



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

