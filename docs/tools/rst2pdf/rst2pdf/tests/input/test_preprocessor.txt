
This file tests the preprocesor extension ("-e preprocess").

This is one paragraph.

This is the next paragraph.  Now, we will test that we can add a
bit of space using the new syntax:

.. space::  40

Now, we're going to test that we can use the syntax to include
both restructured text and stylesheets:

.. include:: test_style_width.txt
.. include:: test_style_width.style

Now we're going to try to do a pagebreak:

.. page::

Now, we're going to test the single word styling:

.. style::

    foobar: {something: something_else}

    singleword: {parent: base, fontSize: 24}

SingleWord

this is a paragraph

AnotherSingleWord

Another paragraph

Finally, we're going to try setting table widths to 20%, 80%, and then the other way around:

.. widths::  20 80

======  ==================
Hi      there
How     are you?
======  ==================

.. widths::  80 20

======  ==================
Hi      there
How     are you?
======  ==================

