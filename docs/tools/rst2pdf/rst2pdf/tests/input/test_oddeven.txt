Test for the oddeven directive. The next construct prints "odd" on odd pages and "even" on even pages:

So, this page is...

.. oddeven::

    .. container:: odd

       odd

    .. container:: even

       even

.. raw:: pdf

   PageBreak
   

And this page is...

.. oddeven::

    .. container:: odd

       odd

    .. container:: even

       even
