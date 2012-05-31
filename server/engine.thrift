
/*
 * Defines ManagerService (Engine), MonitorService (Engine) and SubscriptionService (Web)
 */

exception InvalidOperation {
  1: i32 what,
  2: string why
}

/*
 * ManagerService types
 */
struct FlowletSchema {
  1: map<string, string> inbound,
  2: map<string, string> outbound
}

struct FlowletInstance {
  1: string id
}

struct Flowlet {
  1: string id,
  2: string name,
  3: string className,
  4: FlowletSchema schema,
  5: string status,
  6: list<FlowletInstance> instances
}

struct Flow {
  1: string id,
  2: string name,
  3: string username,
  4: string ns,
  5: string status,
  6: i32 runs,
  7: list<Flowlet> flowlets,
  8: map<string, list<string>> connections
}

struct FlowEvent {
  1: string type,
  2: string result,
  3: string username,
  4: i32 timestamp
}

/*
 * ManagerService encompasses all functionality to get and control flows
 */
service ManagerService {
   void ping(),
   list<Flow> getFlows(1:string ns, 2:i32 offset, 3:i32 limit),
   Flow getFlow(1:string ns, 2:string name),
   bool start(1:string ns, 2:string name) throws (1:InvalidOperation e),
   bool stop(1:string ns, 2:string name) throws (1:InvalidOperation e),
   list<FlowEvent> getActionLog(1:string ns, 2:string name)
}

/*
 * MonitorService types
 */
struct FlowletStatus {
  1: string id,
  2: string status,
  3: map<string, i32> tuples
}

struct FlowStatus {
  1: string id,
  2: string status,
  3: i32 started,
  4: i32 stopped,
  5: i32 runs,
  6: list<FlowletStatus> flowlets
}

/*
 * MonitorService is limited to a FlowStatus request
 */
service MonitorService {
   void ping(),
   FlowStatus getFlowStatus(1:string ns, 2:string name),
   bool subcribe(1:string ns, 2:string name, 3:string endpoint) throws (1:InvalidOperation e),
   bool unsubscribe(1:string id) throws (1:InvalidOperation e)
}

/*
 * SubscriptionService is operated by the Web server to receive PubSub events
 */
service SubscriptionService {
   void ping(),
   bool fire(1:string id, 2:FlowEvent event)
}