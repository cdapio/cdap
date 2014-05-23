.. graphviz::

   digraph g {
     node [shape = record,height=.08];
     node0[label = "header (a5 00)\n2 | data\n12 | checksum\n2 "];
     node1[label = "header (a5 01)\n2 | A | B | C | quick brown fox lazy dogs | checksum\n2 "];
   }