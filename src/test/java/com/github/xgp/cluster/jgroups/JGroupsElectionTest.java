package com.github.xgp.cluster.jgroups;

import static com.github.xgp.cluster.jgroups.TestSupport.RecordingListener.COORD;
import static com.github.xgp.cluster.jgroups.TestSupport.RecordingListener.REMOVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.github.xgp.cluster.Cluster;
import com.github.xgp.cluster.Node;
import com.github.xgp.cluster.election.ElectionStrategy;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class JGroupsElectionTest extends TestSupport {

  private final RecordingListener l1 = new RecordingListener();
  private final RecordingListener l2 = new RecordingListener();
  private final RecordingListener l3 = new RecordingListener();
  private JGroupsCluster c1, c2, c3;

  private void startThree() throws Exception {
    c1 = startCluster("node1", l1);
    c2 = startCluster("node2", l2);
    c3 = startCluster("node3", l3);
    awaitConverged(c1, c2, c3);
  }

  /** Waits until every member flags exactly {@code expected} as the only coordinator. */
  private static void awaitCoordinator(String expected, Cluster... cs) {
    for (Cluster c : cs) {
      await(
          c.getLocalNode().getName() + " agrees coordinator is " + expected,
          10000,
          () -> coordinators(c).equals(Collections.singletonList(expected)));
    }
  }

  private static void assertConsistent(Cluster c) {
    String self = c.getLocalNode().getName();
    List<String> coords = coordinators(c);
    assertEquals(self + " flags exactly one coordinator", 1, coords.size());
    Node entry = nodeNamed(c, self);
    assertEquals(
        self + ": getLocalNode().isCoordinator() agrees with its getNodes() entry",
        entry.isCoordinator(),
        c.getLocalNode().isCoordinator());
    assertEquals(coords.get(0).equals(self), c.getLocalNode().isCoordinator());
  }

  @Test
  public void allMembersAgreeOnExactlyOneCoordinator() throws Exception {
    startThree();
    awaitCoordinator("node3", c1, c2, c3);
    quietPeriod(500); // looped-back broadcasts must not disturb the flags
    for (Cluster c : new Cluster[] {c1, c2, c3}) {
      assertConsistent(c);
      assertEquals(Collections.singletonList("node3"), coordinators(c));
    }
    assertTrue(c3.getLocalNode().isCoordinator());
  }

  /** The smoke-test scenario: after node1 leaves, node2's own LocalNode must stay flagged. */
  @Test
  public void survivorsCoordinatorFlagSticksOnItsLocalNode() throws Exception {
    c1 = startCluster("node1", l1);
    c2 = startCluster("node2", l2);
    awaitConverged(c1, c2);
    c1.stop();
    await("node2 alone", 10000, () -> nodeNames(c2).equals(Collections.singleton("node2")));
    await("ELECTED_COORDINATOR(node2) on node2", 5000, () -> l2.count(COORD, "node2") >= 1);
    quietPeriod(500); // let any looped-back state broadcast land
    assertTrue(c2.getLocalNode().isCoordinator());
    assertTrue(nodeNamed(c2, "node2") == c2.getLocalNode());
    assertConsistent(c2);
  }

  @Test
  public void joinerListenerAddedAfterStartLearnsCoordinator() throws Exception {
    c1 = startCluster("node1");
    c2 = startCluster("node2");
    awaitConverged(c1, c2);
    RecordingListener late = new RecordingListener();
    c1.addClusterListener(late);
    await("late listener on node1 told coordinator", 5000, () -> late.count(COORD, "node2") == 1);
    assertEquals(1, late.count(COORD));
  }

  @Test
  public void coordinatorStopTriggersOneAgreedElection() throws Exception {
    startThree();
    awaitCoordinator("node3", c1, c2, c3);
    quietPeriod(300);
    l1.clear();
    l2.clear();

    c3.stop();
    awaitCoordinator("node2", c1, c2);
    awaitSameView(c1, c2);
    quietPeriod(500);
    for (RecordingListener l : new RecordingListener[] {l1, l2}) {
      assertEquals("one ELECTED_COORDINATOR(node2): " + l, 1, l.count(COORD, "node2"));
      assertEquals("no other elections: " + l, 1, l.count(COORD));
    }
    assertConsistent(c1);
    assertConsistent(c2);
    assertTrue("new coordinator's own LocalNode is flagged", c2.getLocalNode().isCoordinator());
    assertFalse(c1.getLocalNode().isCoordinator());
  }

  @Test
  public void nonCoordinatorStopDoesNotChangeCoordinator() throws Exception {
    startThree();
    awaitCoordinator("node3", c1, c2, c3);
    quietPeriod(300);
    l2.clear();
    l3.clear();

    c1.stop();
    for (RecordingListener l : new RecordingListener[] {l2, l3}) {
      await("REMOVE_NODE(node1) in " + l, 10000, () -> l.count(REMOVE, "node1") == 1);
    }
    quietPeriod(500);
    assertEquals("no election on node2: " + l2, 0, l2.count(COORD));
    assertEquals("no election on node3: " + l3, 0, l3.count(COORD));
    assertEquals(Collections.singletonList("node3"), coordinators(c2));
    assertEquals(Collections.singletonList("node3"), coordinators(c3));
  }

  /** Elects the member with the lowest name. */
  static class LowestNameStrategy implements ElectionStrategy {
    public Node doElection(Cluster cluster) {
      Node elect = cluster.getLocalNode();
      for (Node n : cluster.getNodes().values()) {
        if (n.getName().compareTo(elect.getName()) < 0) {
          elect = n;
        }
      }
      return elect;
    }
  }

  @Test
  public void customStrategyIsHonouredAtTheNextElection() throws Exception {
    startThree();
    awaitCoordinator("node3", c1, c2, c3);
    for (Cluster c : new Cluster[] {c1, c2, c3}) {
      c.setElectionStrategy(new LowestNameStrategy());
    }
    l1.clear();
    l3.clear();

    c2.stop();
    awaitCoordinator("node1", c1, c3);
    await("ELECTED_COORDINATOR(node1) on node1", 5000, () -> l1.count(COORD, "node1") == 1);
    await("ELECTED_COORDINATOR(node1) on node3", 5000, () -> l3.count(COORD, "node1") == 1);
    assertTrue(c1.getLocalNode().isCoordinator());
    assertFalse(c3.getLocalNode().isCoordinator());
  }

  /** Elects the member whose state has the smallest "rank", ties broken by name. */
  static class LowestRankStrategy implements ElectionStrategy {
    public Node doElection(Cluster cluster) {
      Node best = cluster.getLocalNode();
      for (Node n : cluster.getNodes().values()) {
        int r = (Integer) n.getState().getOrDefault("rank", Integer.MAX_VALUE);
        int b = (Integer) best.getState().getOrDefault("rank", Integer.MAX_VALUE);
        if (r < b || (r == b && n.getName().compareTo(best.getName()) < 0)) {
          best = n;
        }
      }
      return best;
    }
  }

  @Test
  public void stateDependentStrategyIsReEvaluatedWhenStateChanges() throws Exception {
    c1 = createCluster("node1");
    c2 = createCluster("node2");
    c1.setElectionStrategy(new LowestRankStrategy());
    c2.setElectionStrategy(new LowestRankStrategy());
    c1.addClusterListener(l1);
    c2.addClusterListener(l2);
    c1.start();
    c2.start();
    awaitConverged(c1, c2);
    awaitCoordinator("node1", c1, c2); // no ranks yet: lowest name

    c2.getLocalNode().setState(java.util.Map.of("rank", 1));
    awaitCoordinator("node2", c1, c2);
    assertTrue(c2.getLocalNode().isCoordinator());

    c1.getLocalNode().setState(java.util.Map.of("rank", 0));
    awaitCoordinator("node1", c1, c2);
    assertTrue(c1.getLocalNode().isCoordinator());
    assertFalse(c2.getLocalNode().isCoordinator());
  }
}
