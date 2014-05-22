Syntax Highlighting
===================

.. highlight:: python
   :linenothreshold: 5

.. code-block:: python

   "Some code without linenos."

.. code-block:: python
   :linenos:

   "Some code with linenos."


.. code-block:: python
   
   "This has less than 5 lines, so no linenos"
   
.. code-block:: python
   
   "This has more than 5 lines, so it has linenos"
   "This has more than 5 lines, so it has linenos"
   "This has more than 5 lines, so it has linenos"
   "This has more than 5 lines, so it has linenos"
   "This has more than 5 lines, so it has linenos"
   "This has more than 5 lines, so it has linenos"
   "This has more than 5 lines, so it has linenos"

::
    
    "This is before a highlight:: none, so it has highlight"


.. highlight:: none

::
    
    "This is after a highlight:: none, so no highlight"
    
.. literalinclude:: conf.py
   :lines: 1,3,10-20
