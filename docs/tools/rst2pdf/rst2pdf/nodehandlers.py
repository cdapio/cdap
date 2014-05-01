# -*- coding: utf-8 -*-
# See LICENSE.txt for licensing terms
#$URL: https://rst2pdf.googlecode.com/svn/tags/0.93/rst2pdf/nodehandlers.py $
#$Date: 2012-02-28 21:07:21 -0300 (Tue, 28 Feb 2012) $
#$Revision: 2443 $

# Import all node handler modules here.
# The act of importing them wires them in.

import genelements
import genpdftext

#sphinxnodes needs these
from genpdftext import NodeHandler, FontHandler, HandleEmphasis

# createpdf needs this
nodehandlers = NodeHandler()
