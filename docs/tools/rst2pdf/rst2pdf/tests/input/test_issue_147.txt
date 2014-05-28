When using a cursor, always use bindings.  `String interpolation
<http://docs.python.org/library/stdtypes.html#string-formatting-operations>`_
may seem more convenient but you will encounter difficulties.  You may
feel that you have complete control over all data accessed but if your
code is at all useful then you will find it being used more and more
widely.  The computer will always be better than you at parsing SQL
and the bad guys have years of experience finding and using `SQL
injection attacks <http://en.wikipedia.org/wiki/SQL_injection>`_ in
ways you never even thought possible.

