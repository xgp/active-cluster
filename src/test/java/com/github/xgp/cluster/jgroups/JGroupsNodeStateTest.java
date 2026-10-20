package com.github.xgp.cluster.jgroups;

import static com.github.xgp.cluster.jgroups.TestSupport.RecordingListener.UPDATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.github.xgp.cluster.Cluster;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class JGroupsNodeStateTest extends TestSupport {

  private static Map<String, Object> state(Object... kv) {
    Map<String, Object> m = new HashMap<>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put((String) kv[i], kv[i + 1]);
    }
    return m;
  }

  private static void awaitState(Cluster observer, String node, Map<String, Object> expected) {
    await(
        observer.getLocalNode().getName() + " sees " + node + " state " + expected,
        5000,
        () -> expected.equals(nodeNamed(observer, node).getState()));
  }

  @Test
  public void setStateIsVisibleEverywhereWithAnUpdateEvent() throws Exception {
    RecordingListener l1 = new RecordingListener(), l3 = new RecordingListener();
    JGroupsCluster c1 = startCluster("node1", l1);
    JGroupsCluster c2 = startCluster("node2");
    JGroupsCluster c3 = startCluster("node3", l3);
    awaitConverged(c1, c2, c3);
    l1.clear();
    l3.clear();

    Map<String, Object> s = state("role", "worker", "load", 7);
    c2.getLocalNode().setState(s);

    awaitState(c1, "node2", s);
    awaitState(c3, "node2", s);
    await("UPDATE_NODE(node2) on node1", 5000, () -> l1.count(UPDATE, "node2") == 1);
    await("UPDATE_NODE(node2) on node3", 5000, () -> l3.count(UPDATE, "node2") == 1);
  }

  @Test
  public void secondSetStateReplacesRatherThanMerges() throws Exception {
    JGroupsCluster c1 = startCluster("node1");
    JGroupsCluster c2 = startCluster("node2");
    awaitConverged(c1, c2);

    c2.getLocalNode().setState(state("a", 1));
    awaitState(c1, "node2", state("a", 1));
    c2.getLocalNode().setState(state("b", 2));
    awaitState(c1, "node2", state("b", 2));
    assertEquals(state("b", 2), c2.getLocalNode().getState());
  }

  @Test
  public void lateJoinerSeesExistingStateImmediately() throws Exception {
    JGroupsCluster c1 = startCluster("node1");
    JGroupsCluster c2 = startCluster("node2");
    awaitConverged(c1, c2);
    Map<String, Object> s = state("ready", true);
    c2.getLocalNode().setState(s);
    awaitState(c1, "node2", s);

    JGroupsCluster c3 = startCluster("node3");
    awaitSameView(c1, c2, c3);
    // state transfer happens inside start(), so no polling here
    assertEquals(s, nodeNamed(c3, "node2").getState());
  }

  @Test
  public void joiningProducesNoUpdateEvents() throws Exception {
    RecordingListener l1 = new RecordingListener(), l2 = new RecordingListener();
    JGroupsCluster c1 = startCluster("node1", l1);
    JGroupsCluster c2 = startCluster("node2", l2);
    awaitConverged(c1, c2);
    quietPeriod(500);
    assertEquals("node1 events: " + l1, 0, l1.count(UPDATE));
    assertEquals("node2 events: " + l2, 0, l2.count(UPDATE));
  }

  @Test
  public void oneRealChangeIsAtMostOneUpdateEvenAcrossRebroadcasts() throws Exception {
    RecordingListener l1 = new RecordingListener();
    RecordingListener l2 = new RecordingListener();
    RecordingListener l3 = new RecordingListener();
    JGroupsCluster c1 = startCluster("node1", l1);
    JGroupsCluster c2 = startCluster("node2", l2);
    JGroupsCluster c3 = startCluster("node3", l3);
    awaitConverged(c1, c2, c3);
    quietPeriod(300);
    l1.clear();
    l2.clear();
    l3.clear();

    Map<String, Object> s = state("v", 1);
    c2.getLocalNode().setState(s);
    await("UPDATE_NODE(node2) on node1", 5000, () -> l1.count(UPDATE, "node2") == 1);
    await("UPDATE_NODE(node2) on node3", 5000, () -> l3.count(UPDATE, "node2") == 1);

    // a join makes members re-broadcast their unchanged state
    JGroupsCluster c4 = startCluster("node4");
    awaitConverged(c1, c2, c3, c4);
    quietPeriod(500);

    assertEquals("node1 events: " + l1, 1, l1.count(UPDATE, "node2"));
    assertEquals("node3 events: " + l3, 1, l3.count(UPDATE, "node2"));
    assertTrue("node2 events: " + l2, l2.count(UPDATE, "node2") <= 1);
    for (RecordingListener l : new RecordingListener[] {l1, l2, l3}) {
      assertEquals("unchanged nodes must not fire updates: " + l, 0, l.count(UPDATE, "node1"));
      assertEquals("unchanged nodes must not fire updates: " + l, 0, l.count(UPDATE, "node3"));
      assertEquals("unchanged nodes must not fire updates: " + l, 0, l.count(UPDATE, "node4"));
    }
    assertEquals(s, nodeNamed(c4, "node2").getState());
  }

  @Test
  public void staleStateNeverOverwritesNewerState() throws Exception {
    RecordingListener l1 = new RecordingListener();
    JGroupsCluster c1 = startCluster("node1", l1);
    JGroupsCluster c2 = startCluster("node2");
    awaitConverged(c1, c2);

    c2.getLocalNode().setState(state("v", 1));
    JGroupsNode v1 = snapshot(c2);
    c2.getLocalNode().setState(state("v", 2));
    awaitState(c1, "node2", state("v", 2));
    l1.clear();

    // a delayed copy of the older state (e.g. a re-broadcast or state transfer that lost a race)
    c1.handleNodeUpdate(
        new com.github.xgp.cluster.ClusterEvent(
            null, v1, com.github.xgp.cluster.ClusterEvent.UPDATE_NODE),
        c2.getChannel().getAddress());
    assertEquals(state("v", 2), nodeNamed(c1, "node2").getState());
    assertEquals("no event for a stale update: " + l1, 0, l1.count(UPDATE));
  }

  @Test
  public void zoneChangePropagates() throws Exception {
    JGroupsCluster c1 = startCluster("node1");
    JGroupsCluster c2 = startCluster("node2");
    awaitConverged(c1, c2);
    ((JGroupsLocalNode) c2.getLocalNode()).setZone("dmz");
    await(
        "node1 sees node2 in zone dmz", 5000, () -> "dmz".equals(nodeNamed(c1, "node2").getZone()));
  }

  /** A detached copy of a member's local node as it would travel on the wire, version included. */
  private static JGroupsNode snapshot(JGroupsCluster c) throws Exception {
    JGroupsLocalNode local = (JGroupsLocalNode) c.getLocalNode();
    JGroupsNode plain =
        new JGroupsNode(
            local.getName(), ((JGroupsDestination) local.getDestination()).getAddress());
    plain.setState(local.getState());
    plain.setVersion(local.getVersion());
    return JGroupsSerializationTest.roundTrip(plain);
  }
}
