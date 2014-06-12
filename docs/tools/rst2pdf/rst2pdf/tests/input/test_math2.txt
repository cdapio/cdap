Bring rst2pdf math support to the level of sphinx's math extension.

Inline Math
~~~~~~~~~~~

Since Pythagoras, we know that :math:`a^2 + b^2 = c^2`.

Math Directive
~~~~~~~~~~~~~~

This below should go in two lines:

.. math::

   (a + b)^2 = a^2 + 2ab + b^2

   (a - b)^2 = a^2 - 2ab + b^2

Aligned equations:
   
.. math::
    
   (a + b)^2  &=  (a + b)(a + b) \\
              &=  a^2 + 2ab + b^2

Simple math can go as argument of the directive              
              
.. math:: (a + b)^2 = a^2 + 2ab + b^2

The :eq:`euler` label should point at this equation:

.. math:: e^{i\pi} + 1 = 0
   :label: euler

.. math::
   :nowrap:

   \begin{eqnarray}
      y    & = & ax^2 + bx + c \\
      f(x) & = & x^2 + 2xy + y^2
   \end{eqnarray}