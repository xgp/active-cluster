# Plan: automated validation of the JGroups implementation

This document is a self-contained brief for an agent (or person) who has not seen this
repository before. It explains what the project is, what is already known to be broken,
what tests to write, what fixes those tests are expected to force, and the order to do
it in. The job is finished when `mvn test` is green five runs in a row and the
multi-process tier passes under its Maven profile.

---

## 1. Orientation

**What the project is.** ActiveCluster is a revival of the old Codehaus cluster API:
a `Cluster` with named `Node`s, replicated per-node state, cluster membership events,
broadcast and direct messaging, and coordinator election. The API lives in
`src/main/java/com/github/xgp/cluster/` and the only implementation is JGroups-based in
`src/main/java/com/github/xgp/cluster/jgroups/`. Read `README.md` first.

**Key files.**

| File | Role |
|---|---|
| `Cluster.java`, `Node.java`, `LocalNode.java`, `ClusterListener.java`, `ClusterEvent.java`, `Message.java`, `MessageConsumer.java` | Public API. Do not change these. |
| `jgroups/JGroupsCluster.java` | The implementation. Implements `Cluster`, and JGroups `Receiver` (view changes, message receipt, state transfer). ~1000 lines, most of the work and most of the bugs are here. |
| `jgroups/JGroupsNode.java`, `JGroupsLocalNode.java` | Node model. `Externalizable`. `JGroupsLocalNode extends JGroupsNode`. |
| `jgroups/JGroupsDestination.java` | Destination = name + transient JGroups `Address`, equality by name only. |
| `jgroups/JGroupsMessage.java`, `JGroupsMessageConsumer.java` | Message + consumer with a trivial `k='v'` selector. |
| `jgroups/JGroupsClusterFactory.java` | Factory; takes a JGroups XML config name resolved from the classpath. |
| `election/impl/BullyElectionStrategy.java` | Default election strategy. |
| `group/*` | Pure in-memory group/buddy model, no JGroups dependency. |
| `examples/JGroupsClusterExample.java` | Interactive stdin-driven demo: `send`, `sendto`, `nodes`, `quit`. |
| `src/main/resources/{minimal,simple-udp,udp,tcp}.xml` | JGroups stacks. UDP ones need multicast. |

**Build.** Java 21 target (JDK 25 works), Maven 3.9, parent POM `com.github.xgp:oss-parent:0.7`.
`mvn -q compile` works. JUnit 4.13.2 is declared with test scope but there is **no**
`src/test` directory and **no** surefire configuration yet. JGroups is 5.3.4.Final.

**How messaging works internally (needed to understand the defects).**
- Broadcast: `send(clusterDestination, msg)` wraps the `Message` in a JGroups
  `ObjectMessage` with `dest=null` and also hand-delivers it to local consumers.
- Direct: `send(nodeDestination, msg)` sets property `__direct_to=<nodeName>` and
  multicasts it; every node's `receive()` drops it unless the name matches.
- Node state: `broadcastNodeState()` multicasts a `ClusterEvent(UPDATE_NODE, localNode)`
  as a `BytesMessage`; receivers call `handleNodeUpdate`, which does
  `nodes.put(node.getDestination(), node)`.
- Membership: `viewAccepted(View)` diffs the view against `nodes`, creating placeholder
  `JGroupsNode`s for new addresses (name from the static `org.jgroups.util.NameCache`),
  removing departed ones, electing a coordinator if `coordinator == null`, and then
  re-broadcasting local state.
- Joining: `start()` calls `channel.connect()`, then `channel.getState(null, 10000)`
  which pulls the existing members' `nodes` map via `getState/setState` streams.

---

## 2. What is already known (from a two-node smoke test)

A throwaway test was run with two `JGroupsCluster` instances in one JVM over JGroups'
in-process `SHARED_LOOPBACK` transport (config in section 3.1). Observed:

**Works today**
- Both nodes converge to the same `getNodes()` name set.
- `ADD_NODE` fires on each side; clean `stop()` on node1 produces `REMOVE_NODE(node1)`
  then `ELECTED_COORDINATOR(node2)` on node2.
- Direct message node1 -> node2 is delivered exactly once, only to node2's local-node consumer.
- `LocalNode.setState` on node2 becomes visible in node1's `getNodes()`.
- Java serialization of `JGroupsMessage`, `ClusterEvent`, `JGroupsNode`,
  `JGroupsDestination` crosses the channel without any JGroups serialization filter issue.

**Defects found (the tests below must catch every one of these)**

| # | Defect | Where / why |
|---|---|---|
| D1 | One broadcast from node1 reached node1's own consumer **twice** (node2 once). | `JGroupsCluster.send()` hand-delivers locally, and JGroups also loops the multicast back into `receive()` because `discard_own_messages` is false. |
| D2 | After node1 left and node2 fired `ELECTED_COORDINATOR(node2)`, `node2.getLocalNode().isCoordinator()` was **false**. | node2's own state broadcast loops back; `handleNodeUpdate` replaces the real `JGroupsLocalNode` in `nodes` with a deserialized copy. `electCoordinator` flags whichever object is in the map; the next loopback puts the other one back. Object identity in `nodes` is unstable. |
| D3 | node1's entry for node2 is a deserialized **`JGroupsLocalNode`** (with `cluster == null`), not a `JGroupsNode`. | `broadcastNodeState` serializes the local node object itself. `copyAsRemoteNode()` exists but is only used by state transfer. So `getNodes().values()` contains objects that are `instanceof LocalNode` but are not local. |
| D4 | A joining node never gets a coordinator until someone leaves; `ELECTED_COORDINATOR` fires inside `start()` before a listener can be added (README adds the listener after `start()`). | `electCoordinator()` only runs when `coordinator == null`; first node sets itself in `start()`. Also `started` is false during `channel.connect()`, so the very first `viewAccepted` is discarded. |
| D5 | `BullyElectionStrategy` javadoc says "lowest lexicographical name", the code picks the **highest**. | `compareTo(...) < 0` branch. Highest is the textbook bully rule and matches the original Codehaus code. |
| D6 | A single join produced four `UPDATE_NODE(node1)` events on node1. | `viewAccepted` re-broadcasts state on every view change and the broadcast loops back; no change detection. |
| D7 | Small things: `ClusterEvent.getTypeAsString()` lacks `ELECTED_COORDINATOR`; `stop()` clears `clusterListeners` so a restarted cluster silently loses them; `handleViewChange` iterates the synchronized `nodes` map without its lock. | |

---

## 3. Test design

All tests go under `src/test/java/com/github/xgp/cluster/...` and run from `mvn test`.
Three tiers. Tier 1 is the iteration loop; tiers 2 and 3 confirm the result on a real
transport and across JVM boundaries. Keep JUnit 4. Add no new test-scope dependencies;
a small helper class covers waiting and recording.

### 3.1 Tier 1: in-JVM contract tests over SHARED_LOOPBACK (always on)

**Config** `src/test/resources/test-loopback.xml` (verified working with this code):

```xml
<config xmlns="urn:org:jgroups" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="urn:org:jgroups http://www.jgroups.org/schema/jgroups.xsd">
  <SHARED_LOOPBACK/>
  <SHARED_LOOPBACK_PING/>
  <MERGE3 min_interval="1000" max_interval="3000"/>
  <FD_ALL3 timeout="3000" interval="1000"/>
  <VERIFY_SUSPECT timeout="500"/>
  <pbcast.NAKACK2/>
  <UNICAST3/>
  <pbcast.STABLE/>
  <pbcast.GMS join_timeout="1000"/>
  <UFC/>
  <MFC/>
  <FRAG2/>
  <pbcast.STATE_TRANSFER/>
</config>
```

No sockets, no multicast; runs on any machine and in CI.

**Helper** `TestSupport` (one class):
- `startCluster(name, clusterName)` using `new JGroupsClusterFactory("test-loopback.xml")`;
  keeps a list and stops everything in reverse order in `@After`.
- Unique cluster name per test method (`"t-" + methodName + "-" + System.nanoTime()`)
  so SHARED_LOOPBACK state from one test can never bleed into another.
- `RecordingListener implements ClusterListener`: appends `"TYPE:nodeName"` strings to a
  `CopyOnWriteArrayList`, with `count(type, nodeName)` helpers.
- `QueueListener implements MessageListener` backed by a `LinkedBlockingQueue<Message>`.
- `await(String what, long timeoutMs, BooleanSupplier cond)` polling at ~50 ms, failing
  with `what` in the message. Never use bare `Thread.sleep` to wait for a condition;
  only use a fixed sleep when asserting that something does **not** happen.
- `awaitSameView(clusters...)`: calls
  `org.jgroups.util.Util.waitUntilAllChannelsHaveSameView(timeout, 100, channels)`
  using `((JGroupsCluster) c).getChannel()`.
- `nodeNamed(cluster, name)` to look up a `Node` from `getNodes()`.
- Simulated crash: `org.jgroups.util.Util.shutdown(channel)` kills a channel without a
  graceful LEAVE so failure detection has to notice it.

**Test classes and contracts**

`JGroupsSerializationTest` (pure, no channels)
- Round-trip via `ObjectOutputStream`/`ObjectInputStream`: `JGroupsMessage` (id, props,
  body), `JGroupsNode` (name, state, zone, coordinator), `JGroupsLocalNode`,
  `JGroupsDestination` with a non-null `Address` (address survives through
  `Util.writeAddress`/`readAddress`), `ClusterEvent` (type + node).
- `JGroupsDestination` equals/hashCode by name only, regardless of address.
- `JGroupsMessageConsumer.matchesSelector`: `null` selector matches all; `k='v'` and
  `k=v` match a property `k=v`; mismatch returns false; missing property returns false.

`GroupModelTest` (pure)
- `GroupModel` with min 2 / max 3: adding 1 node gives an unusable group, 2 usable, 3 full,
  4th node starts a new group; `removeNode` moves groups back; `MasterZoneFilter`
  blocks non-matching nodes from becoming masters (they go to the unused pool);
  `BuddyGroupModel.addNode` creates one group per node and fills buddies;
  `GroupClusterListener` forwards add/remove/fail to the model.

`JGroupsLifecycleTest`
- `start()` twice is a no-op; `stop()` twice is a no-op; `stop()` then `start()` works.
- `send` and `createConsumer` before `start()` throw `ClusterException`.
- `stop()` closes open consumers; `receive()` on a closed consumer throws.
- Single node: `getLocalNode().isCoordinator()` is true; a listener added **before**
  `start()` sees `ELECTED_COORDINATOR(node1)`; a listener added **after** `start()`
  also learns the coordinator (see fix F3).
- `getNodes()` is unmodifiable; `Node.getState()` is unmodifiable.
- Listeners added before `stop()` still receive events after a restart (D7).

`JGroupsMembershipTest`
- 2 then 3 nodes: `awaitSameView`, then every node's `getNodes()` key set equals
  `{node1,node2,node3}`.
- Each observer gets exactly one `ADD_NODE` per remote node (no duplicates, none for self).
- No value in any `getNodes()` other than the local one is `instanceof LocalNode` (D3).
- Clean `stop()` of node2: each survivor gets exactly one `REMOVE_NODE(node2)`, no
  `FAILED_NODE`, and node2 leaves `getNodes()`.
- Crash of node3 via `Util.shutdown(channel)`: survivors get `FAILED_NODE(node3)` then
  `REMOVE_NODE(node3)` within ~10 s (FD_ALL3 3 s + VERIFY_SUSPECT 0.5 s + slack).

`JGroupsMessagingTest`
- 3 nodes, one cluster-destination consumer each. One broadcast from node1: each
  consumer receives exactly **one** message, including node1's own (D1). Assert "exactly
  one" by waiting for 1 then sleeping 500 ms and asserting the count is still 1.
- `noLocal=true` consumer on node1 receives nothing from node1's broadcast but does
  receive node2's.
- Direct message node1 -> node2: node2's local-node consumer gets exactly one;
  node1's and node3's get nothing; node2's cluster-destination consumer gets nothing.
- Direct message to self (node1 -> node1) is delivered once.
- Selector `kind='a'`: consumer sees messages with property `kind=a` and not `kind=b`.
- Two consumers on the same destination on the same node both receive.
- Queue mode: no listener set, `receive(timeout)` returns the message; `receiveNoWait()`
  returns null when empty.
- Body round-trip: `String`, a `HashMap`, and a 1 MB `byte[]` (exercises FRAG2).
  `getBody(WrongType.class)` returns null.
- Ordering: 100 broadcasts from node1 with property `seq=i` arrive on node2 in order,
  no loss, no duplicates.

`JGroupsNodeStateTest`
- `setState` on node2 -> node1 and node3 see it in `getNodes()` and get an
  `UPDATE_NODE(node2)` event.
- A second `setState` replaces the map (old keys gone), not merges.
- A node that joins **after** node2 set its state sees the state immediately after
  `awaitSameView` (state-transfer path, `setState(InputStream)`).
- Event hygiene (D6): after the cluster is stable, one `setState` on node2 causes at
  most one `UPDATE_NODE(node2)` per observer, and no `UPDATE_NODE(node2)` on node2 for
  its own unchanged re-broadcasts. Decide the exact rule when fixing F2; the test pins
  "at most one per real change".

`JGroupsElectionTest`
- 3 nodes after `awaitSameView`: on every node, exactly one value of `getNodes()` has
  `isCoordinator() == true`, and it is the same name on all three (D2, D4).
- The local node's `getLocalNode().isCoordinator()` agrees with its own `getNodes()` entry.
- Coordinator stops: survivors agree on a new coordinator and each fires exactly one
  `ELECTED_COORDINATOR` for it.
- A non-coordinator stops: the coordinator does not change and no
  `ELECTED_COORDINATOR` fires.
- `setElectionStrategy(lowest-name strategy)` on all nodes before a membership change:
  the next election honours it.
- `BullyElectionStrategyTest` (pure): pins the rule chosen in F4 with a fake `Cluster`.

### 3.2 Tier 2: real TCP transport in one JVM (always on, one class)

`src/test/resources/test-tcp.xml`: copy `src/main/resources/tcp.xml` and set
`TCP bind_addr="127.0.0.1" bind_port="17800" port_range="20"`,
`TCPPING initial_hosts="127.0.0.1[17800]" port_range="20"`, `FD_ALL3 timeout="3000"
interval="1000"`, `GMS join_timeout="1000"`.

`JGroupsTcpTransportTest`: one 3-node scenario reusing `TestSupport` with the TCP
config: join, broadcast once each, direct once, `setState`, clean leave, and the same
exactly-once assertions. This catches what SHARED_LOOPBACK hides (real fragmentation,
bundling, physical-address resolution).

### 3.3 Tier 3: separate JVMs through the example app (Maven profile `multiprocess`)

`JGroupsMultiProcessIT` (run by failsafe, class name ends in `IT`):
- Launches 2 or 3 copies of `com.github.xgp.cluster.examples.JGroupsClusterExample`
  with `ProcessBuilder`, `java -cp <target/classes>:<jgroups jar from
  java.class.path> ... JGroupsClusterExample nodeN test-tcp.xml`, each with
  `-Djgroups.bind_addr=127.0.0.1 -Djava.net.preferIPv4Stack=true`.
- Drives each process over stdin with the existing commands `send <text>`,
  `sendto <node> <text>`, `nodes`, `quit`; reads stdout on a thread into a list.
- Asserts on the example's own output lines: `[CLUSTER] Node added: node2`,
  `[MESSAGE from node1] hi` appearing exactly once per receiving process,
  `[DIRECT from node1] hi` only on the target, `(coordinator)` on exactly one line of
  `nodes` output and the same name in every process, `[CLUSTER] Node removed:` after `quit`.
- This is the only tier that proves cross-JVM logical-name resolution (the
  `NameCache` used in `handleViewChange` is static per JVM) and that the README's
  documented flow actually works. Off by default because it needs free ports and ~20 s.

---

## 4. Build changes (`pom.xml`)

- Add `maven-surefire-plugin` 3.2.5: `forkCount=1`, `reuseForks=false`,
  `trimStackTrace=false`, `systemPropertyVariables`:
  `java.net.preferIPv4Stack=true`, `jgroups.bind_addr=127.0.0.1`,
  `java.util.logging.config.file=${project.basedir}/src/test/resources/logging.properties`.
- `src/test/resources/logging.properties`: `com.github.xgp.level=INFO`,
  `org.jgroups.level=WARNING`, console handler. `JGroupsCluster` logs at INFO per
  message; consider demoting the per-message lines to FINE as part of the fixes.
- Profile `multiprocess` adding `maven-failsafe-plugin` 3.2.5 with `integration-test`
  and `verify` goals (default `*IT` include).
- Keep `junit:junit:4.13.2`. Do not add Awaitility, AssertJ, Mockito, or JUnit 5.

---

## 5. Fixes the tests are expected to force

Make only the changes a failing test demands. Never weaken an assertion to go green;
if a contract in section 3 seems wrong, say so in the final report rather than
silently changing it.

| Fix | Defect | What to do |
|---|---|---|
| F1 | D1 | In `send()` for the cluster destination, delete the manual loop that calls `consumer.onMessage` locally. JGroups loops own multicasts back into `receive()`, which already handles `noLocal`. Keep direct-message behaviour (it already relies on loopback). |
| F2 | D2, D3, D6 | `broadcastNodeState` sends `copyAsRemoteNode(localNode)` instead of the local node. `handleNodeUpdate`: if the incoming name is the local node's name, ignore it (or only refresh the address); otherwise if a node with that destination exists, update its state/zone/coordinator **in place** rather than replacing the object, and fire `UPDATE_NODE` only if state actually changed. Object identity in `nodes` must be stable so election flags stick. |
| F3 | D4 | Set `started = true` before `channel.connect()` (or explicitly process `channel.getView()` after connect) so the first view is handled. Run election on **every** view change using the view's member names as the candidate set (all nodes see identical views, so they agree), not only when `coordinator == null`. Fire `ELECTED_COORDINATOR` only when the winner changes. When a listener is added after start and a coordinator is known, deliver it an `ELECTED_COORDINATOR` immediately, so the README's add-listener-after-start pattern works. |
| F4 | D5 | Recommended: keep "highest name wins" (textbook bully, matches the original code) and fix the javadoc. If the user prefers lowest, flip the comparison instead. Either way `BullyElectionStrategyTest` pins it. |
| F5 | D7 | Add `ELECTED_COORDINATOR` to `ClusterEvent.getTypeAsString()`. Stop clearing `clusterListeners` in `stop()`. In `handleViewChange`/`suspect`, snapshot `nodes` inside `synchronized (nodes)` before iterating. |
| F6 | hygiene | Only if a test exposes them: the unused `RpcDispatcher`/`RequestHandler` code and the four-config fallback ladder in `start()`. Otherwise leave them alone. |

Out of scope: changing any interface in `com.github.xgp.cluster`, replacing Java
serialization, network-partition/merge tests (would need a DISCARD-protocol harness;
note it as a follow-up), and a CI workflow file (a one-job GitHub Actions `mvn -B test`
can be added afterwards).

---

## 6. Execution order and iteration loop

Work in this order. After each step: run, read failures, fix the implementation, re-run
until green, commit.

1. Build scaffolding: `pom.xml` surefire + profile, `logging.properties`,
   `test-loopback.xml`, `test-tcp.xml`, `TestSupport`. `mvn -q test` runs zero tests cleanly.
2. Pure tests: `JGroupsSerializationTest`, `GroupModelTest`, `BullyElectionStrategyTest`.
3. `JGroupsLifecycleTest`, `JGroupsMembershipTest` (expect F3, F5).
4. `JGroupsMessagingTest` (expect F1).
5. `JGroupsNodeStateTest`, `JGroupsElectionTest` (expect F2, F3, F4).
6. `JGroupsTcpTransportTest`.
7. `JGroupsMultiProcessIT` under `mvn -q verify -Pmultiprocess`.
8. Flakiness pass: `for i in 1 2 3 4 5; do mvn -q test || break; done`. Fix flakes by
   tightening waits and convergence checks, never by adding sleeps or retries.
9. `mvn fmt:format` (plugin already configured), update README "Building and Running"
   with `mvn test` and `mvn verify -Pmultiprocess`, final commit.

**Commit rules for this repo.** Author must be `xgp <244253+xgp@users.noreply.github.com>`
(check `git config user.name`/`user.email`; set them locally if different). No
`Co-Authored-By`, no "Generated with" footer, no AI attribution anywhere. One commit per
step above with a plain message describing the change.

**Done criteria.**
- `mvn -q test` passes five consecutive runs, total under ~60 s.
- `mvn -q verify -Pmultiprocess` passes.
- Every defect D1 to D7 has a test that failed before its fix and passes after.
- Final report lists: tests added, fixes made with file references, any contract from
  section 3 that was changed and why, and anything left out.

---

## Appendix: the smoke test that produced section 2

Verified to compile against `target/classes` plus the JGroups jar and run in ~8 s.
Useful as a starting point for `TestSupport`.

```java
import com.github.xgp.cluster.*;
import com.github.xgp.cluster.jgroups.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class Smoke {
  static class L implements ClusterListener {
    final List<String> events = new CopyOnWriteArrayList<>();
    void rec(String s, ClusterEvent e) { events.add(s + ":" + e.getNode().getName()); }
    public void onNodeAdd(ClusterEvent e) { rec("ADD", e); }
    public void onNodeUpdate(ClusterEvent e) { rec("UPDATE", e); }
    public void onNodeRemoved(ClusterEvent e) { rec("REMOVED", e); }
    public void onNodeFailed(ClusterEvent e) { rec("FAILED", e); }
    public void onCoordinatorChanged(ClusterEvent e) { rec("COORD", e); }
  }
  public static void main(String[] a) throws Exception {
    JGroupsClusterFactory f = new JGroupsClusterFactory("test-loopback.xml");
    Cluster c1 = f.createCluster("node1", "t"); L l1 = new L(); c1.addClusterListener(l1); c1.start();
    Cluster c2 = f.createCluster("node2", "t"); L l2 = new L(); c2.addClusterListener(l2); c2.start();
    Thread.sleep(1500);
    AtomicInteger b1 = new AtomicInteger(), b2 = new AtomicInteger(), d2 = new AtomicInteger();
    c1.createConsumer(c1.getDestination()).setMessageListener(m -> b1.incrementAndGet());
    c2.createConsumer(c2.getDestination()).setMessageListener(m -> b2.incrementAndGet());
    c2.createConsumer(c2.getLocalNode().getDestination()).setMessageListener(m -> d2.incrementAndGet());
    c1.send(c1.getDestination(), new JGroupsMessage("hello"));
    Thread.sleep(1000);
    System.out.println("b1=" + b1 + " b2=" + b2);           // observed: b1=2 b2=1  (D1)
    Node n2 = c1.getNodes().values().stream().filter(n -> n.getName().equals("node2")).findFirst().get();
    System.out.println(n2.getClass().getSimpleName());       // observed: JGroupsLocalNode (D3)
    c1.send(n2.getDestination(), new JGroupsMessage("direct"));
    Thread.sleep(1000);
    System.out.println("d2=" + d2);                          // observed: 1
    c1.stop(); Thread.sleep(2000);
    System.out.println(l2.events);                           // ... REMOVED:node1, COORD:node2 ...
    System.out.println(c2.getLocalNode().isCoordinator());   // observed: false (D2)
    c2.stop();
    System.exit(0);
  }
}
```
