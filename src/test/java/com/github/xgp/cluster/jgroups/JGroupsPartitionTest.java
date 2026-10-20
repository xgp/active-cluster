package com.github.xgp.cluster.jgroups;

import static com.github.xgp.cluster.jgroups.TestSupport.RecordingListener.ADD;
import static com.github.xgp.cluster.jgroups.TestSupport.RecordingListener.COORD;
import static com.github.xgp.cluster.jgroups.TestSupport.RecordingListener.FAILED;
import static com.github.xgp.cluster.jgroups.TestSupport.RecordingListener.REMOVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.github.xgp.cluster.Cluster;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import org.junit.Test;

/**
 * Network partitions (simulated with JGroups DISCARD) and their healing (JGroups MERGE3): each side
 * must carry on as a cluster of its own, and after the merge every member must agree again on
 * membership, coordinator and state.
 */
public class JGroupsPartitionTest extends TestSupport {

  @Override
  protected String config() {
    return PARTITION_CONFIG;
  }

  private static final long SPLIT_TIMEOUT = 15000;
  private static final long MERGE_TIMEOUT = 30000;

  private final RecordingListener l1 = new RecordingListener();
  private final RecordingListener l2 = new RecordingListener();
  private final RecordingListener l3 = new RecordingListener();
  private JGroupsCluster c1, c2, c3;

  private void startThree() throws Exception {
    c1 = startCluster("node1", l1);
    c2 = startCluster("node2", l2);
    c3 = startCluster("node3", l3);
    awaitConverged(c1, c2, c3);
    awaitCoordinator("node3", c1, c2, c3);
    quietPeriod(300);
    l1.clear();
    l2.clear();
    l3.clear();
  }

  private static void awaitCoordinator(String expected, Cluster... cs) {
    for (Cluster c : cs) {
      await(
          c.getLocalNode().getName() + " agrees coordinator is " + expected,
          MERGE_TIMEOUT,
          () -> coordinators(c).equals(Collections.singletonList(expected)));
    }
  }

  private static Map<String, Object> state(String k, Object v) {
    Map<String, Object> m = new HashMap<>();
    m.put(k, v);
    return m;
  }

  private static void awaitState(Cluster observer, String node, Map<String, Object> expected) {
    await(
        observer.getLocalNode().getName() + " sees " + node + " state " + expected,
        MERGE_TIMEOUT,
        () ->
            nodeNamed(observer, node) != null
                && expected.equals(nodeNamed(observer, node).getState()));
  }

  private static TreeSet<String> names(String... n) {
    return new TreeSet<>(Arrays.asList(n));
  }

  @Test
  public void eachSideOfAPartitionCarriesOnAndTheMergeReconciles() throws Exception {
    startThree();
    Tap t1 = tap(c1), t2 = tap(c2), t3 = tap(c3);

    partition(new Cluster[] {c1, c2}, new Cluster[] {c3});
    awaitViewSize(2, SPLIT_TIMEOUT, c1, c2);
    awaitViewSize(1, SPLIT_TIMEOUT, c3);
    awaitNodeNames(names("node1", "node2"), c1, c2);
    awaitNodeNames(names("node3"), c3);

    // each side saw the other side fail, and the majority side elected a new coordinator
    for (RecordingListener l : new RecordingListener[] {l1, l2}) {
      await("node3 failed then removed: " + l, 5000, () -> l.count(REMOVE, "node3") == 1);
      assertEquals(l.toString(), 1, l.count(FAILED, "node3"));
      assertTrue(l.toString(), l.indexOf(FAILED, "node3") < l.indexOf(REMOVE, "node3"));
      assertEquals(l.toString(), 1, l.count(COORD, "node2"));
    }
    for (String gone : new String[] {"node1", "node2"}) {
      await("node3 saw " + gone + " fail: " + l3, 5000, () -> l3.count(REMOVE, gone) == 1);
      assertEquals(l3.toString(), 1, l3.count(FAILED, gone));
    }
    assertEquals("isolated node3 stays its own coordinator: " + l3, 0, l3.count(COORD));
    awaitCoordinator("node2", c1, c2);
    awaitCoordinator("node3", c3);

    // both sides keep working and change state independently
    QueueListener q1 = new QueueListener();
    c1.createConsumer(c1.getDestination()).setMessageListener(q1);
    QueueListener q3 = new QueueListener();
    c3.createConsumer(c3.getDestination()).setMessageListener(q3);
    c2.send(c2.getDestination(), new JGroupsMessage("majority"));
    await("majority side broadcast", 5000, () -> q1.size() == 1);
    c1.getLocalNode().setState(state("side", "majority"));
    c3.getLocalNode().setState(state("side", "minority"));
    awaitState(c2, "node1", state("side", "majority"));
    quietPeriod(300);
    assertEquals("nothing crosses the partition", 0, q3.size());

    l1.clear();
    l2.clear();
    l3.clear();
    heal(c1, c2, c3);

    awaitViewSize(3, MERGE_TIMEOUT, c1, c2, c3);
    awaitSameView(c1, c2, c3);
    awaitNodeNames(names("node1", "node2", "node3"), c1, c2, c3);
    assertTrue(
        "the healed view came from a JGroups merge",
        t1.mergeViews.get() + t2.mergeViews.get() + t3.mergeViews.get() > 0);

    // returning members are announced once, and the coordinator is agreed again
    awaitCoordinator("node3", c1, c2, c3);
    for (RecordingListener l : new RecordingListener[] {l1, l2}) {
      assertEquals(l.toString(), 1, l.count(ADD, "node3"));
      await("node3 elected again: " + l, 5000, () -> l.count(COORD, "node3") == 1);
    }
    assertEquals(l3.toString(), 1, l3.count(ADD, "node1"));
    assertEquals(l3.toString(), 1, l3.count(ADD, "node2"));
    assertTrue(c3.getLocalNode().isCoordinator());
    assertTrue(!c1.getLocalNode().isCoordinator() && !c2.getLocalNode().isCoordinator());

    // state changed on either side during the partition is visible everywhere
    awaitState(c1, "node3", state("side", "minority"));
    awaitState(c2, "node3", state("side", "minority"));
    awaitState(c3, "node1", state("side", "majority"));

    // and messaging spans the whole cluster again, each message exactly once
    q1.messages.clear();
    c3.send(c3.getDestination(), new JGroupsMessage("healed"));
    QueueListener d1 = new QueueListener();
    c1.createConsumer(c1.getLocalNode().getDestination()).setMessageListener(d1);
    c3.send(nodeNamed(c3, "node1").getDestination(), new JGroupsMessage("direct"));
    await("broadcast after merge", 5000, () -> q1.size() == 1 && q3.size() == 1);
    await("direct after merge", 5000, () -> d1.size() == 1);
    quietPeriod(500);
    assertEquals(1, q1.size());
    assertEquals(1, q3.size());
    assertEquals(1, d1.size());
  }

  @Test
  public void isolatingTheJGroupsCoordinatorAlsoHeals() throws Exception {
    // node1 started first, so it is the JGroups view coordinator: cut it off alone
    startThree();
    partition(new Cluster[] {c1}, new Cluster[] {c2, c3});
    awaitViewSize(1, SPLIT_TIMEOUT, c1);
    awaitViewSize(2, SPLIT_TIMEOUT, c2, c3);
    awaitCoordinator("node1", c1);
    awaitCoordinator("node3", c2, c3);
    assertEquals("node2/node3 side keeps its coordinator: " + l2, 0, l2.count(COORD));
    await("node1 elects itself", 5000, () -> l1.count(COORD, "node1") == 1);

    heal(c1, c2, c3);
    awaitViewSize(3, MERGE_TIMEOUT, c1, c2, c3);
    awaitNodeNames(names("node1", "node2", "node3"), c1, c2, c3);
    awaitCoordinator("node3", c1, c2, c3);
    await("node1 told node3 is coordinator again", 5000, () -> l1.count(COORD, "node3") == 1);
  }

  @Test
  public void repeatedPartitionsDoNotLeakMembersOrEvents() throws Exception {
    final int rounds = 2;
    startThree();
    for (int round = 0; round < rounds; round++) {
      partition(new Cluster[] {c1, c2}, new Cluster[] {c3});
      awaitViewSize(2, SPLIT_TIMEOUT, c1, c2);
      awaitViewSize(1, SPLIT_TIMEOUT, c3);
      heal(c1, c2, c3);
      awaitViewSize(3, MERGE_TIMEOUT, c1, c2, c3);
      awaitNodeNames(names("node1", "node2", "node3"), c1, c2, c3);
    }
    awaitCoordinator("node3", c1, c2, c3);
    quietPeriod(500);
    // every loss was matched by exactly one return
    assertEquals(l1.toString(), rounds, l1.count(REMOVE, "node3"));
    assertEquals(l1.toString(), rounds, l1.count(ADD, "node3"));
    assertEquals(l3.toString(), rounds, l3.count(REMOVE, "node1"));
    assertEquals(l3.toString(), rounds, l3.count(ADD, "node1"));
    for (Cluster c : new Cluster[] {c1, c2, c3}) {
      assertEquals(3, c.getNodes().size());
    }
  }
}
