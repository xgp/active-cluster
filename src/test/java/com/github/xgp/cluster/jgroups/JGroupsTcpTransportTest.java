package com.github.xgp.cluster.jgroups;

import static com.github.xgp.cluster.jgroups.TestSupport.RecordingListener.ADD;
import static com.github.xgp.cluster.jgroups.TestSupport.RecordingListener.FAILED;
import static com.github.xgp.cluster.jgroups.TestSupport.RecordingListener.REMOVE;
import static com.github.xgp.cluster.jgroups.TestSupport.RecordingListener.UPDATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.github.xgp.cluster.Cluster;
import com.github.xgp.cluster.Message;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.jgroups.protocols.TCP;
import org.junit.Test;

/** The same contracts over a real TCP stack on 127.0.0.1 (fragmentation, bundling, addressing). */
public class JGroupsTcpTransportTest extends TestSupport {

  @Override
  protected String config() {
    return TCP_CONFIG;
  }

  private static Set<String> senders(QueueListener q) {
    Set<String> s = new HashSet<>();
    for (Message m : q.list()) {
      s.add((String) m.getProperty("from"));
    }
    return s;
  }

  @Test
  public void threeNodeScenarioOverTcp() throws Exception {
    RecordingListener[] ls = {
      new RecordingListener(), new RecordingListener(), new RecordingListener()
    };
    JGroupsCluster[] cs = new JGroupsCluster[3];
    QueueListener[] bq = new QueueListener[3];
    QueueListener[] dq = new QueueListener[3];
    for (int i = 0; i < 3; i++) {
      cs[i] = startCluster("node" + (i + 1), ls[i]);
      bq[i] = new QueueListener();
      cs[i].createConsumer(cs[i].getDestination()).setMessageListener(bq[i]);
      dq[i] = new QueueListener();
      cs[i].createConsumer(cs[i].getLocalNode().getDestination()).setMessageListener(dq[i]);
    }
    for (JGroupsCluster c : cs) {
      assertEquals(
          "must really run on TCP",
          TCP.class,
          c.getChannel().getProtocolStack().getTransport().getClass());
    }
    awaitConverged(cs);
    awaitCoordinatorAgreed(cs);

    // join: one ADD per remote node, none for self
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 3; j++) {
        assertEquals(ls[i].toString(), i == j ? 0 : 1, ls[i].count(ADD, "node" + (j + 1)));
      }
    }

    // one broadcast from each member, including a 200 KB body to force fragmentation
    byte[] big = new byte[200 * 1024];
    for (int i = 0; i < 3; i++) {
      Message m = new JGroupsMessage(i == 0 ? big : "hi");
      m.setProperty("from", "node" + (i + 1));
      cs[i].send(cs[i].getDestination(), m);
    }
    for (int i = 0; i < 3; i++) {
      QueueListener q = bq[i];
      await("3 broadcasts on node" + (i + 1), 10000, () -> q.size() >= 3);
    }

    // one direct message node1 -> node3
    Message direct = new JGroupsMessage("direct");
    direct.setProperty("from", "node1");
    cs[0].send(nodeNamed(cs[0], "node3").getDestination(), direct);
    await("direct on node3", 10000, () -> dq[2].size() >= 1);

    // state change on node2
    Map<String, Object> state = new HashMap<>();
    state.put("tcp", "yes");
    cs[1].getLocalNode().setState(state);
    for (int i : new int[] {0, 2}) {
      Cluster c = cs[i];
      await(
          "node2 state on node" + (i + 1),
          10000,
          () -> state.equals(nodeNamed(c, "node2").getState()));
    }

    quietPeriod(500);
    Set<String> all = new HashSet<>();
    Collections.addAll(all, "node1", "node2", "node3");
    for (int i = 0; i < 3; i++) {
      assertEquals("exactly 3 broadcasts on node" + (i + 1), 3, bq[i].size());
      assertEquals(all, senders(bq[i]));
    }
    assertEquals(1, dq[2].size());
    assertEquals(0, dq[0].size());
    assertEquals(0, dq[1].size());
    assertEquals(1, ls[0].count(UPDATE, "node2"));
    assertEquals(1, ls[2].count(UPDATE, "node2"));

    // clean leave of node2
    cs[1].stop();
    for (int i : new int[] {0, 2}) {
      RecordingListener l = ls[i];
      await("REMOVE_NODE(node2) on node" + (i + 1), 10000, () -> l.count(REMOVE, "node2") == 1);
    }
    awaitSameView(cs[0], cs[2]);
    quietPeriod(500);
    for (int i : new int[] {0, 2}) {
      assertEquals(ls[i].toString(), 1, ls[i].count(REMOVE, "node2"));
      assertEquals(ls[i].toString(), 0, ls[i].count(FAILED));
      assertNull(nodeNamed(cs[i], "node2"));
    }
  }

  private static void awaitCoordinatorAgreed(Cluster... cs) {
    for (Cluster c : cs) {
      await(
          c.getLocalNode().getName() + " agrees on node3 as coordinator",
          10000,
          () -> coordinators(c).equals(Collections.singletonList("node3")));
    }
  }
}
