package com.github.xgp.cluster.election;

import static org.junit.Assert.assertEquals;

import com.github.xgp.cluster.Cluster;
import com.github.xgp.cluster.ClusterListener;
import com.github.xgp.cluster.Destination;
import com.github.xgp.cluster.LocalNode;
import com.github.xgp.cluster.Message;
import com.github.xgp.cluster.MessageConsumer;
import com.github.xgp.cluster.Node;
import com.github.xgp.cluster.election.impl.BullyElectionStrategy;
import com.github.xgp.cluster.jgroups.JGroupsLocalNode;
import com.github.xgp.cluster.jgroups.JGroupsNode;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Test;

/** Pins the bully rule: the member with the highest (lexicographically last) name wins. */
public class BullyElectionStrategyTest {

  /** Minimal Cluster exposing just a local node and a node map. */
  static class FakeCluster implements Cluster {
    final LocalNode local;
    final Map<Destination, Node> nodes = new LinkedHashMap<>();

    FakeCluster(String localName, String... others) {
      local = new JGroupsLocalNode(localName);
      nodes.put(local.getDestination(), local);
      for (String o : others) {
        JGroupsNode n = new JGroupsNode(o);
        nodes.put(n.getDestination(), n);
      }
    }

    public Destination getDestination() {
      return null;
    }

    public Map<Destination, Node> getNodes() {
      return Collections.unmodifiableMap(nodes);
    }

    public void addClusterListener(ClusterListener l) {}

    public void removeClusterListener(ClusterListener l) {}

    public LocalNode getLocalNode() {
      return local;
    }

    public void setElectionStrategy(ElectionStrategy s) {}

    public void send(Destination d, Message m) {}

    public MessageConsumer createConsumer(Destination d) {
      return null;
    }

    public MessageConsumer createConsumer(Destination d, String s) {
      return null;
    }

    public MessageConsumer createConsumer(Destination d, String s, boolean n) {
      return null;
    }

    public void start() {}

    public void stop() {}
  }

  private static String elect(String local, String... others) throws Exception {
    return new BullyElectionStrategy().doElection(new FakeCluster(local, others)).getName();
  }

  @Test
  public void highestNameWins() throws Exception {
    assertEquals("charlie", elect("alpha", "charlie", "bravo"));
    assertEquals("charlie", elect("bravo", "alpha", "charlie"));
    assertEquals("charlie", elect("charlie", "alpha", "bravo"));
  }

  @Test
  public void allMembersAgreeRegardlessOfWhoAsks() throws Exception {
    assertEquals(elect("node1", "node2", "node3"), elect("node3", "node1", "node2"));
    assertEquals(elect("node2", "node3", "node1"), elect("node1", "node2", "node3"));
  }

  @Test
  public void aloneElectsSelf() throws Exception {
    assertEquals("solo", elect("solo"));
  }
}
