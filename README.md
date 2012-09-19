data-fabric
===========
Queues, Operations and Search

This is depending on a custom release of hbase. We keep the hbase patch is in this directory for reference, it can serve as a base for future patches. 
The current patch is HBASE-DF-NATIVE-QUEUES-1.3.4.patch and it is based on:

hbase andreas$ git show
commit 3abd441376501780c6f54fc8c9befe979cced6e3
Author: Jimmy Xiang <jxiang@apache.org>
Date:   Tue Sep 18 17:04:49 2012 +0000

    HBASE-6803 script hbase should add JAVA_LIBRARY_PATH to LD_LIBRARY_PATH
    
    git-svn-id: https://svn.apache.org/repos/asf/hbase/branches/0.94@1387260 13f79535-47bb-0310-9956-ffa450edef68


