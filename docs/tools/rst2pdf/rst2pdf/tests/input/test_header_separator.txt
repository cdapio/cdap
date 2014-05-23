.. header::

   This should appear in the header.

.. footer::

   This should appear in the footer.

The objective of this file is to test transitions.  The restructured text specification says that:

    The syntax for a transition marker is a horizontal line of 4 or more repeated punctuation characters.
    The syntax is the same as section title underlines without title text. Transition markers require blank
    lines before and after.

    ...

    The processing system is free to render transitions in output in any way it likes. For example,
    horizontal rules (<hr>) in HTML output would be an obvious choice.

So, let's try a transition here.

----

And this should be after it.

We also put a transition in the header and one in the footer, but docutils crashes on doing it
that way, so we add "headerSeparator" and "footerSeparator" to the page template in the stylesheet.

