A simple formula:

.. math:: \alpha > \beta


One with a few horizontal lines:

.. math::

   \frac{2 \pm \sqrt{7}}{3}


Inline replacements works, too: |AA| <-- there should be an alpha + beta

You can use a math role: :math:`\alpha + \beta`  but it has some problems 
with spacing and if things are not one-line: :math:`\alpha^2`.

Don't push it, the directive just works better: :math:`\frac{2 \pm \sqrt{7}}{3}`

.. |AA| math:: \alpha + \beta^2

