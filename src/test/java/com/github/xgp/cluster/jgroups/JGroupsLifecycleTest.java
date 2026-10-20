package com.github.xgp.cluster.jgroups;

import static com.github.xgp.cluster.jgroups.TestSupport.RecordingListener.ADD;
import static com.github.xgp.cluster.jgroups.TestSupport.RecordingListener.COORD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.github.xgp.cluster.ClusterException;
import com.github.xgp.cluster.Destination;
import com.github.xgp.cluster.MessageConsumer;
import com.github.xgp.cluster.Node;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.jgroups.JChannel;
import org.junit.Test;

public class JGroupsLifecycleTest extends TestSupport {

  @Test
  public void startTwiceIsNoOp() throws Exception {
    JGroupsCluster c = startCluster("node1");
    JChannel channel = c.getChannel();
    c.start();
    assertSame(channel, c.getChannel());
    assertEquals(Collections.singleton("node1"), nodeNames(c));
  }

  @Test
  public void startWithUnloadableConfigFailsInsteadOfFallingBack() throws Exception {
    JGroupsCluster c =
        (JGroupsCluster)
            new JGroupsClusterFactory("no-such-config.xml").createCluster("node1", clusterName());
    try {
      c.start();
      c.stop();
      fail("a named config that cannot be loaded must fail start(), not silently fall back");
    } catch (ClusterException expected) {
    }
    try {
      c.send(c.getDestination(), new JGroupsMessage("x"));
      fail("a cluster whose start() failed is not started");
    } catch (ClusterException expected) {
    }
  }

  @Test
  public void stopTwiceIsNoOp() throws Exception {
    JGroupsCluster c = startCluster("node1");
    c.stop();
    c.stop();
  }

  @Test
  public void stopThenStartWorks() throws Exception {
    JGroupsCluster c = startCluster("node1");
    c.stop();
    c.start();
    assertEquals(Collections.singleton("node1"), nodeNames(c));
    assertTrue(c.getLocalNode().isCoordinator());

    QueueListener q = new QueueListener();
    c.createConsumer(c.getDestination()).setMessageListener(q);
    c.send(c.getDestination(), new JGroupsMessage("after-restart"));
    await("broadcast after restart", 5000, () -> q.size() >= 1);
  }

  @Test
  public void sendBeforeStartThrows() throws Exception {
    JGroupsCluster c = createCluster("node1");
    try {
      c.send(c.getDestination(), new JGroupsMessage("x"));
      fail("send before start must throw");
    } catch (ClusterException expected) {
    }
  }

  @Test
  public void createConsumerBeforeStartThrows() throws Exception {
    JGroupsCluster c = createCluster("node1");
    try {
      c.createConsumer(c.getDestination());
      fail("createConsumer before start must throw");
    } catch (ClusterException expected) {
    }
  }

  @Test
  public void stopClosesConsumers() throws Exception {
    JGroupsCluster c = startCluster("node1");
    MessageConsumer consumer = c.createConsumer(c.getDestination());
    assertEquals(null, consumer.receiveNoWait());
    c.stop();
    try {
      consumer.receive(10);
      fail("receive on a consumer closed by stop() must throw");
    } catch (Exception expected) {
    }
  }

  @Test
  public void receiveOnClosedConsumerThrows() throws Exception {
    JGroupsCluster c = startCluster("node1");
    MessageConsumer consumer = c.createConsumer(c.getDestination());
    consumer.close();
    try {
      consumer.receiveNoWait();
      fail("receive on a closed consumer must throw");
    } catch (Exception expected) {
    }
  }

  @Test
  public void singleNodeIsCoordinatorAndListenerAddedBeforeStartIsTold() throws Exception {
    RecordingListener l = new RecordingListener();
    JGroupsCluster c = startCluster("node1", l);
    assertTrue(c.getLocalNode().isCoordinator());
    await("ELECTED_COORDINATOR(node1)", 5000, () -> l.count(COORD, "node1") == 1);
    assertEquals("one election only: " + l, 1, l.count(COORD));
  }

  @Test
  public void listenerAddedAfterStartLearnsCoordinator() throws Exception {
    JGroupsCluster c = startCluster("node1");
    RecordingListener l = new RecordingListener();
    c.addClusterListener(l);
    await("late listener told ELECTED_COORDINATOR(node1)", 5000, () -> l.count(COORD) == 1);
    assertEquals(1, l.count(COORD, "node1"));
  }

  @Test
  public void getNodesAndNodeStateAreUnmodifiable() throws Exception {
    JGroupsCluster c = startCluster("node1");
    Map<String, Object> state = new HashMap<>();
    state.put("k", "v");
    c.getLocalNode().setState(state);

    Map<Destination, Node> nodes = c.getNodes();
    try {
      nodes.clear();
      fail("getNodes() must be unmodifiable");
    } catch (UnsupportedOperationException expected) {
    }
    Node n = nodeNamed(c, "node1");
    assertNotNull(n);
    try {
      n.getState().put("x", "y");
      fail("Node.getState() must be unmodifiable");
    } catch (UnsupportedOperationException expected) {
    }
    assertEquals("v", c.getLocalNode().getState().get("k"));
  }

  @Test
  public void listenersSurviveRestart() throws Exception {
    RecordingListener l = new RecordingListener();
    JGroupsCluster c1 = startCluster("node1", l);
    c1.stop();
    l.clear();
    c1.start();
    await("listener still registered after restart", 5000, () -> l.count(COORD, "node1") >= 1);

    startCluster("node2");
    await("ADD_NODE(node2) after restart", 10000, () -> l.count(ADD, "node2") == 1);
  }
}
