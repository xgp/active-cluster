package com.github.xgp.cluster.jgroups;

import static com.github.xgp.cluster.jgroups.TestSupport.RecordingListener.ADD;
import static com.github.xgp.cluster.jgroups.TestSupport.RecordingListener.FAILED;
import static com.github.xgp.cluster.jgroups.TestSupport.RecordingListener.REMOVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.github.xgp.cluster.Cluster;
import com.github.xgp.cluster.LocalNode;
import com.github.xgp.cluster.Node;
import java.util.Arrays;
import java.util.TreeSet;
import org.junit.Test;

public class JGroupsMembershipTest extends TestSupport {

  private final RecordingListener l1 = new RecordingListener();
  private final RecordingListener l2 = new RecordingListener();
  private final RecordingListener l3 = new RecordingListener();
  private JGroupsCluster c1, c2, c3;

  private void startThree() throws Exception {
    c1 = startCluster("node1", l1);
    c2 = startCluster("node2", l2);
    awaitConverged(c1, c2);
    c3 = startCluster("node3", l3);
    awaitConverged(c1, c2, c3);
  }

  @Test
  public void twoThenThreeNodesConverge() throws Exception {
    c1 = startCluster("node1", l1);
    c2 = startCluster("node2", l2);
    awaitConverged(c1, c2);
    assertEquals(new TreeSet<>(Arrays.asList("node1", "node2")), nodeNames(c1));
    assertEquals(nodeNames(c1), nodeNames(c2));

    c3 = startCluster("node3", l3);
    awaitConverged(c1, c2, c3);
    for (Cluster c : new Cluster[] {c1, c2, c3}) {
      assertEquals(new TreeSet<>(Arrays.asList("node1", "node2", "node3")), nodeNames(c));
    }
  }

  @Test
  public void exactlyOneAddPerRemoteNodeAndNoneForSelf() throws Exception {
    startThree();
    quietPeriod(500);
    RecordingListener[] ls = {l1, l2, l3};
    String[] names = {"node1", "node2", "node3"};
    for (int i = 0; i < ls.length; i++) {
      for (int j = 0; j < names.length; j++) {
        int expected = i == j ? 0 : 1;
        assertEquals(
            names[i] + " ADD_NODE(" + names[j] + ") count, events " + ls[i],
            expected,
            ls[i].count(ADD, names[j]));
      }
    }
  }

  @Test
  public void onlyTheLocalEntryIsALocalNode() throws Exception {
    startThree();
    quietPeriod(500); // let looped-back state broadcasts land
    for (Cluster c : new Cluster[] {c1, c2, c3}) {
      String self = c.getLocalNode().getName();
      for (Node n : c.getNodes().values()) {
        if (n.getName().equals(self)) {
          assertSame(self + "'s own entry must be its LocalNode", c.getLocalNode(), n);
        } else {
          assertFalse(
              self + " holds remote " + n.getName() + " as a LocalNode", n instanceof LocalNode);
        }
      }
    }
  }

  @Test
  public void cleanStopFiresOneRemoveAndNoFailure() throws Exception {
    startThree();
    l1.clear();
    l3.clear();
    c2.stop();
    for (RecordingListener l : new RecordingListener[] {l1, l3}) {
      await("REMOVE_NODE(node2) in " + l, 10000, () -> l.count(REMOVE, "node2") == 1);
    }
    awaitSameView(c1, c3);
    quietPeriod(500);
    for (RecordingListener l : new RecordingListener[] {l1, l3}) {
      assertEquals("exactly one REMOVE_NODE(node2): " + l, 1, l.count(REMOVE, "node2"));
      assertEquals("no FAILED_NODE on clean stop: " + l, 0, l.count(FAILED));
    }
    assertNull(nodeNamed(c1, "node2"));
    assertNull(nodeNamed(c3, "node2"));
  }

  @Test
  public void crashFiresFailedThenRemove() throws Exception {
    startThree();
    l1.clear();
    l2.clear();
    crash(c3);
    for (RecordingListener l : new RecordingListener[] {l1, l2}) {
      await(
          "FAILED_NODE(node3) and REMOVE_NODE(node3) in " + l,
          10000,
          () -> l.count(FAILED, "node3") == 1 && l.count(REMOVE, "node3") == 1);
      assertTrue(
          "FAILED_NODE must precede REMOVE_NODE: " + l,
          l.indexOf(FAILED, "node3") < l.indexOf(REMOVE, "node3"));
    }
    assertNull(nodeNamed(c1, "node3"));
    assertNull(nodeNamed(c2, "node3"));
  }
}
