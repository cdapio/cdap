#!/bin/bash
for i in `seq 1 10`; do 
curl  -X POST -d '{"host" : "abc.net", "component" : "app-fabric", "logline":"2013-10-07 10:44:51,489 - WARN  [main:o.a.h.c.Configuration@816] - hadoop.native.lib is deprecated. Instead, use io.native.lib.available"}' "http://localhost:10000/v2/streams/event-stream"
curl  -X POST -d '{"host" : "abc.net", "component" : "app-fabric", "logline":"2013-10-07 10:34:51,489 - ERROR [main:o.a.h.c.Configuration@816] - hadoop.native.lib is deprecated. Instead, use io.native.lib.available"}' "http://localhost:10000/v2/streams/event-stream";
curl  -X POST -d '{"host" : "abc.net", "component" : "app-fabric", "logline":"2013-10-07 10:24:51,489 - INFO  [main:o.a.h.c.Configuration@816] - hadoop.native.lib is deprecated. Instead, use io.native.lib.available"}' "http://localhost:10000/v2/streams/event-stream";
curl  -X POST -d '{"host" : "abc.net", "component" : "app-fabric", "logline":"2013-10-07 10:32:51,489 - WARN  [main:o.a.h.c.Configuration@816] - hadoop.native.lib is deprecated. Instead, use io.native.lib.available"}' "http://localhost:10000/v2/streams/event-stream"
curl  -X POST -d '{"host" : "abc.net", "component" : "app-fabric", "logline":"2013-10-07 10:28:51,489 - ERROR [main:o.a.h.c.Configuration@816] - hadoop.native.lib is deprecated. Instead, use io.native.lib.available"}' "http://localhost:10000/v2/streams/event-stream";
curl  -X POST -d '{"host" : "abc.net", "component" : "app-fabric", "logline":"2013-10-07 10:30:51,489 - ERROR [main:o.a.h.c.Configuration@816] - hadoop.native.lib is deprecated. Instead, use io.native.lib.available"}' "http://localhost:10000/v2/streams/event-stream";
done


