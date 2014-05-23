The valid units are:

"em" "ex" "px" "in" "cm" "mm" "pt" "pc" "%" "".

16px with 16DPI (defined in the image) == 1 inch

.. image:: images/biohazard_16dpi.png

300, no unit == 300px at 300DPI == 1 inch

.. image:: images/biohazard.png
   :width: 300

1in == 1 inch

.. image:: images/biohazard.png
   :width: 1in

2.54cm == 1 inch

.. image:: images/biohazard.png
   :width: 2.54cm

25.4mm == 1 inch

.. image:: images/biohazard.png
   :width: 25.4mm

72pt == 1 inch

.. image:: images/biohazard.png
   :width: 72pt

7.2em with a 10pt base font == 1 inch

.. image:: images/biohazard.png
   :width: 7.2em

We use the broken IE definition of ex because it's easier.
So, 14.4 ex == 7.2 em with 10pt base font == 1 inch

.. image:: images/biohazard.png
   :width: 14.4ex

1pc == 12pt == 1/6th inch, so 6pc == 1inch

.. image:: images/biohazard.png
   :width: 6pc

All the above images should be exactly 1 inch wide.

.. image:: images/biohazard.png
   :width: 100%

The above image should be exactly the width of the text area

