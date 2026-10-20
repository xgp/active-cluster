# ActiveCluster

ActiveCluster is a small Java API for writing clustered applications, with an implementation on
[JGroups](http://www.jgroups.org). A process joins a named cluster and can then:

- see the current members and the state each one publishes,
- be told when members join, change state, leave or fail,
- send messages to the whole cluster or to one member,
- know which member is the elected **coordinator**, to run singleton work or make decisions,
- arrange members into master/buddy **groups** for failover.

It keeps working through crashes and network partitions, and reconciles when a partition heals.

## Contents

- [Quick start](#quick-start)
- [Concepts](#concepts)
- [Examples](#examples)
- [Configuration](#configuration)
- [How it works](#how-it-works)
- [Building and testing](#building-and-testing)
- [History](#history)

## Quick start

Requires Java 21 or later. The library is not yet on Maven Central; install it locally with
`mvn install` and depend on it:

```xml
<dependency>
  <groupId>com.github.xgp</groupId>
  <artifactId>active-cluster</artifactId>
  <version>0.1-SNAPSHOT</version>
</dependency>
```

Every process creates a cluster member with a unique node name and the shared cluster name:

```java
JGroupsClusterFactory factory = new JGroupsClusterFactory("tcp.xml");
Cluster cluster = factory.createCluster("node-a", "orders");

cluster.addClusterListener(new ClusterListener() {
  public void onNodeAdd(ClusterEvent e) {
    System.out.println("joined " + e.getNode().getName());
  }
  public void onNodeUpdate(ClusterEvent e) {
    System.out.println(e.getNode().getName() + " is now " + e.getNode().getState());
  }
  public void onNodeRemoved(ClusterEvent e) {
    System.out.println("left " + e.getNode().getName());
  }
  public void onNodeFailed(ClusterEvent e) {
    System.out.println("failed " + e.getNode().getName());
  }
  public void onCoordinatorChanged(ClusterEvent e) {
    System.out.println("coordinator " + e.getNode().getName());
  }
});

cluster.start();

MessageConsumer consumer = cluster.createConsumer(cluster.getDestination());
consumer.setMessageListener(m -> System.out.println("got " + m.getBody(String.class)));

cluster.getLocalNode().setState(Map.of("role", "worker", "capacity", 8));
cluster.send(cluster.getDestination(), new JGroupsMessage("hello, everyone"));

// ... later
cluster.stop();
```

Add listeners before `start()` to see every event. A listener added later is still told the
current coordinator straight away.

## Concepts

| Concept | API | What it is |
|---|---|---|
| Cluster | `Cluster` | This process's membership of a named cluster. `start()`, `stop()`, and start again. |
| Node | `Node` | One member: a unique name, a state map, a zone, and whether it is the coordinator. |
| Local node | `LocalNode` | The member in this process. `setState` replaces and replicates its state. |
| Destination | `Destination` | `cluster.getDestination()` addresses everyone; `node.getDestination()` addresses one member. |
| Events | `ClusterListener` | `ADD_NODE`, `UPDATE_NODE`, `REMOVE_NODE`, `FAILED_NODE`, `ELECTED_COORDINATOR`. |
| Consumer | `MessageConsumer` | Receives messages for one destination, pushed to a listener or queued. |
| Election | `ElectionStrategy` | Chooses the coordinator. The default picks the highest node name. |
| Groups | `GroupModel` | Arranges members into groups of a master plus buddies. |

What each event means:

- **`ADD_NODE`**: a member joined, or came back after a partition healed.
- **`UPDATE_NODE`**: a member published a changed state or zone. Duplicate or out-of-date copies
  are ignored, so each real change fires once.
- **`REMOVE_NODE`**: a member left. On its own it means a clean `stop()`.
- **`FAILED_NODE`**: a member crashed or became unreachable. `REMOVE_NODE` follows.
- **`ELECTED_COORDINATOR`**: a different member is now the coordinator.

No event other than `ELECTED_COORDINATOR` is ever fired about the local member.

## Examples

### Direct messages and queues

A message sent to a node's destination goes to that member only. A consumer without a listener
queues messages, and a selector filters on a message property:

```java
Node target = null;
for (Node n : cluster.getNodes().values()) {
  if (n.getName().equals("node-b")) {
    target = n;
  }
}
Message job = new JGroupsMessage(Map.of("id", 42));
job.setProperty("type", "job");
cluster.send(target.getDestination(), job);
```

On `node-b`:

```java
MessageConsumer inbox = cluster.createConsumer(cluster.getLocalNode().getDestination(), "type='job'");
Message received = inbox.receive(5000); // null if nothing arrives in 5 s
```

Sending to a node that is not a current member throws `ClusterException`. Pass `noLocal = true`
to `createConsumer` to ignore broadcasts sent by this member.

### Running work on one member only

Every member is told when the coordinator changes, so each can start or stop a singleton job:

```java
cluster.addClusterListener(new ClusterListenerAdapter() {
  @Override
  public void onCoordinatorChanged(ClusterEvent e) {
    boolean leader = cluster.getLocalNode().isCoordinator();
    scheduler.set(leader); // start or stop the singleton job
  }
});
```

During a network partition each side elects its own coordinator. If the job must never run twice,
also require a quorum, for example `cluster.getNodes().size() > expectedSize / 2`.

### Choosing the coordinator

Any deterministic rule works, because every member runs it over the same members and state. This
one picks the longest-running member, using a start time each member publishes in its state:

```java
cluster.setElectionStrategy(c -> {
  Node oldest = c.getLocalNode();
  for (Node n : c.getNodes().values()) {
    long started = (Long) n.getState().getOrDefault("started", Long.MAX_VALUE);
    long best = (Long) oldest.getState().getOrDefault("started", Long.MAX_VALUE);
    if (started < best || (started == best && n.getName().compareTo(oldest.getName()) < 0)) {
      oldest = n;
    }
  }
  return oldest;
});
cluster.getLocalNode().setState(Map.of("started", System.currentTimeMillis()));
```

Set the same strategy on every member. The election re-runs whenever membership or any member's
state changes.

### Groups

`GroupClusterListener` keeps a `GroupModel` up to date from cluster events. Members fill a group
up to its maximum size; the first member is the master and the rest are its buddies.

```java
GroupModel model = new GroupModel();
model.setMinimumMemberCount(2); // a group is usable with a master and one buddy
model.setMaximumMemberCount(3);
cluster.addClusterListener(new GroupClusterListener(model));
```

The local member does not raise `ADD_NODE`; call `model.addNode(cluster.getLocalNode())` if it
belongs in the model too. `BuddyGroupModel` instead gives every member its own group, with other
members as its buddies. `MasterZoneFilter` restricts which zones may hold masters.

All of the samples above are compiled and run by `ReadmeExamplesTest`.

### The example application

`JGroupsClusterExample` is an interactive member that prints cluster events. Start several in
separate terminals:

```bash
mvn -q compile exec:java -Dexec.args="node1 tcp.xml"
mvn -q compile exec:java -Dexec.args="node2 tcp.xml"
```

Then type `send <text>`, `sendto <node> <text>`, `nodes` or `quit`.

## Configuration

`JGroupsClusterFactory` takes the name of a JGroups XML stack, as a file path or a classpath
resource. With no name it uses `udp.xml`. A stack that cannot be loaded makes `start()` fail;
nothing is silently substituted.

| Stack | Use when |
|---|---|
| `udp.xml` (default) | IP multicast works between the hosts, as on most LANs. |
| `tcp.xml` | Multicast is unavailable, as in most clouds and containers. Members find each other through `TCPPING`. |
| Your own file | You need other discovery (`DNS_PING`, `KUBE_PING`, `JDBC_PING`), encryption or tuned timeouts. |

`udp.xml` and `tcp.xml` are the stacks bundled with, and maintained for, the JGroups version in
use. Both read system properties:

| Property | Stack | Meaning |
|---|---|---|
| `jgroups.bind_addr` | both | Interface to bind, for example `127.0.0.1` or `site_local` |
| `jgroups.tcpping.initial_hosts` | `tcp.xml` | Members to contact first, for example `10.0.0.1[7800],10.0.0.2[7800]` |
| `jgroups.udp.mcast_addr`, `jgroups.udp.mcast_port` | `udp.xml` | Multicast group; change them to keep clusters apart |
| `java.net.preferIPv4Stack` | both | Set to `true` when IPv6 causes binding problems |

A custom stack must include `pbcast.STATE_TRANSFER` so joining members receive the current state,
and a merge protocol such as `MERGE3` so partitions heal. Logging uses `java.util.logging`
under `com.github.xgp.cluster`.

## How it works

- **Membership** follows the JGroups view. A member that calls `stop()` announces it before
  leaving, so survivors can tell a clean leave from a failure.
- **State** is owned by each member and versioned. Copies are applied only if newer, so state never
  rolls back, even across partitions.
- **Election** runs on every member over the same data, so all members agree without extra
  messages.
- **Broadcasts** are JGroups multicasts; **direct messages** are JGroups unicasts to the target's
  current address.
- **Partitions** split the cluster into sides that each keep working. When the network heals,
  JGroups merges the views, returning members are announced with `ADD_NODE`, and state is
  exchanged.

[docs/architecture.md](docs/architecture.md) describes the design in full, including event
ordering, threading, the wire format and what partition handling does not guarantee.

Points to keep in mind:

- Payloads use Java serialization. Run clusters on trusted networks, and consider JGroups
  `ENCRYPT` and `AUTH` and a JVM-wide `jdk.serialFilter`.
- Callbacks run on JGroups threads. Return quickly, and never call `start()` or `stop()` from a
  callback.
- Do not modify a message after sending it.
- The selector supports one property equality, not full SQL-92.

## Building and testing

```bash
mvn install                    # build, test, check formatting
mvn test                       # unit and in-JVM cluster tests, about a minute
mvn verify -Pmultiprocess      # also runs the example app in three JVMs over TCP
mvn fmt:format                 # format the code; verify fails on unformatted code
```

The in-JVM tests use JGroups' `SHARED_LOOPBACK` transport, so they need no network. Partitions
are simulated with the JGroups `DISCARD` protocol. One test runs over real TCP on 127.0.0.1, and
the multiprocess tier uses ports 17800 to 17820.

A release build attaches sources and javadoc: `mvn -Preleases verify`, with
`-Drelease.sign.skip=true` to skip GPG signing.

## History

This is based on a long dead project of the same name by Codehaus. It was somewhat JMS-specific, but I liked the API, and had a working implementation ~20 years ago. I'm trying to bring this back from the bit rot, having not done anything with JGroups for that long. I'm also using it as a test to see if the language models (e.g. Claude Code) are any good at writing the implementation bits for a concise interface.

## License

[Apache License 2.0](LICENSE).
