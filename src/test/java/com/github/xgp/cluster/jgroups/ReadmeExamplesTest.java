package com.github.xgp.cluster.jgroups;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.github.xgp.cluster.Cluster;
import com.github.xgp.cluster.ClusterEvent;
import com.github.xgp.cluster.ClusterListener;
import com.github.xgp.cluster.ClusterListenerAdapter;
import com.github.xgp.cluster.Message;
import com.github.xgp.cluster.MessageConsumer;
import com.github.xgp.cluster.Node;
import com.github.xgp.cluster.group.Group;
import com.github.xgp.cluster.group.GroupClusterListener;
import com.github.xgp.cluster.group.GroupModel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

/**
 * The code samples from README.md, compiled and run so the documentation cannot drift from the API.
 * Keep each block in sync with the README section named in its comment.
 */
public class ReadmeExamplesTest extends TestSupport {

  /** README "Quick start", minus the System.out calls. */
  @Test
  public void quickStart() throws Exception {
    List<String> seen = new CopyOnWriteArrayList<>();

    // --- README: Quick start ---
    JGroupsClusterFactory factory = new JGroupsClusterFactory(config()); // README: "tcp.xml"
    Cluster cluster = factory.createCluster("node-a", clusterName()); // README: "orders"

    cluster.addClusterListener(
        new ClusterListener() {
          public void onNodeAdd(ClusterEvent e) {
            seen.add("joined " + e.getNode().getName());
          }

          public void onNodeUpdate(ClusterEvent e) {
            seen.add(e.getNode().getName() + " is now " + e.getNode().getState());
          }

          public void onNodeRemoved(ClusterEvent e) {
            seen.add("left " + e.getNode().getName());
          }

          public void onNodeFailed(ClusterEvent e) {
            seen.add("failed " + e.getNode().getName());
          }

          public void onCoordinatorChanged(ClusterEvent e) {
            seen.add("coordinator " + e.getNode().getName());
          }
        });

    cluster.start();

    MessageConsumer consumer = cluster.createConsumer(cluster.getDestination());
    consumer.setMessageListener(m -> seen.add("got " + m.getBody(String.class)));

    cluster.getLocalNode().setState(Map.of("role", "worker", "capacity", 8));
    cluster.send(cluster.getDestination(), new JGroupsMessage("hello, everyone"));
    // --- end ---

    await("own broadcast", 5000, () -> seen.contains("got hello, everyone"));
    assertTrue(seen.toString(), seen.contains("coordinator node-a"));

    Cluster other = startCluster("node-b");
    await("join seen", 10000, () -> seen.contains("joined node-b"));
    other.getLocalNode().setState(Map.of("role", "standby"));
    await("update seen", 5000, () -> seen.contains("node-b is now {role=standby}"));
    other.stop();
    await("leave seen", 10000, () -> seen.contains("left node-b"));

    cluster.stop();
  }

  /** README "Direct messages and queues". */
  @Test
  public void directMessagesAndQueues() throws Exception {
    Cluster cluster = startCluster("node-a");
    Cluster b = startCluster("node-b");
    awaitConverged(cluster, b);
    MessageConsumer inbox = b.createConsumer(b.getLocalNode().getDestination(), "type='job'");

    // --- README: Direct messages and queues ---
    Node target = null;
    for (Node n : cluster.getNodes().values()) {
      if (n.getName().equals("node-b")) {
        target = n;
      }
    }
    Message job = new JGroupsMessage(Map.of("id", 42));
    job.setProperty("type", "job");
    cluster.send(target.getDestination(), job);
    // --- end ---

    // --- README: on node-b ---
    Message received = inbox.receive(5000); // null if nothing arrives in 5 s
    // --- end ---

    assertNotNull(received);
    assertEquals(Map.of("id", 42), received.getBody(Map.class));
  }

  /** README "Running work on one member only". */
  @Test
  public void singletonWorkOnTheCoordinator() throws Exception {
    AtomicBoolean[] running = {new AtomicBoolean(), new AtomicBoolean()};
    Cluster[] members = new Cluster[2];
    for (int i = 0; i < 2; i++) {
      AtomicBoolean scheduler = running[i];
      Cluster cluster = createCluster(i == 0 ? "node-a" : "node-b");
      members[i] = cluster;

      // --- README: Running work on one member only ---
      cluster.addClusterListener(
          new ClusterListenerAdapter() {
            @Override
            public void onCoordinatorChanged(ClusterEvent e) {
              boolean leader = cluster.getLocalNode().isCoordinator();
              scheduler.set(leader); // start or stop the singleton job
            }
          });
      // --- end ---

      cluster.start();
    }
    awaitConverged(members);
    await("exactly one scheduler", 5000, () -> running[0].get() != running[1].get());
    assertTrue("highest name wins by default", running[1].get());
  }

  /** README "Choosing the coordinator". */
  @Test
  public void customElection() throws Exception {
    Cluster a = createCluster("node-a");
    Cluster b = createCluster("node-b");
    for (Cluster cluster : new Cluster[] {a, b}) {
      // --- README: Choosing the coordinator ---
      cluster.setElectionStrategy(
          c -> {
            Node oldest = c.getLocalNode();
            for (Node n : c.getNodes().values()) {
              long started = (Long) n.getState().getOrDefault("started", Long.MAX_VALUE);
              long best = (Long) oldest.getState().getOrDefault("started", Long.MAX_VALUE);
              if (started < best
                  || (started == best && n.getName().compareTo(oldest.getName()) < 0)) {
                oldest = n;
              }
            }
            return oldest;
          });
      // --- end ---
    }
    a.getLocalNode().setState(Map.of("started", 1L));
    b.getLocalNode().setState(Map.of("started", 2L));
    a.start();
    b.start();
    awaitConverged(a, b);
    await("node-a is coordinator on node-a", 5000, () -> a.getLocalNode().isCoordinator());
    await("node-a is coordinator on node-b", 5000, () -> nodeNamed(b, "node-a").isCoordinator());
    assertTrue(!b.getLocalNode().isCoordinator());
  }

  /** README "Groups". */
  @Test
  public void groups() throws Exception {
    Cluster cluster = createCluster("node-a");

    // --- README: Groups ---
    GroupModel model = new GroupModel();
    model.setMinimumMemberCount(2); // a group is usable with a master and one buddy
    model.setMaximumMemberCount(3);
    cluster.addClusterListener(new GroupClusterListener(model));
    // --- end ---

    cluster.start();
    startCluster("node-b");
    startCluster("node-c");
    await(
        "one full group of remote members",
        10000,
        () ->
            model.getGroups().size() == 1
                && ((Group) model.getGroups().get(0)).getMemberCount() == 2);
    assertTrue(((Group) model.getGroups().get(0)).isUsable());
  }
}
