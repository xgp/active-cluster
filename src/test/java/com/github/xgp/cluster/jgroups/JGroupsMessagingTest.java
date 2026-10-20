package com.github.xgp.cluster.jgroups;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import com.github.xgp.cluster.ClusterException;
import com.github.xgp.cluster.Message;
import com.github.xgp.cluster.MessageConsumer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;

public class JGroupsMessagingTest extends TestSupport {

  private JGroupsCluster c1, c2, c3;

  @Before
  public void startThree() throws Exception {
    c1 = startCluster("node1");
    c2 = startCluster("node2");
    c3 = startCluster("node3");
    awaitConverged(c1, c2, c3);
  }

  private static QueueListener listen(MessageConsumer consumer) throws Exception {
    QueueListener q = new QueueListener();
    consumer.setMessageListener(q);
    return q;
  }

  private static QueueListener clusterConsumer(JGroupsCluster c) throws Exception {
    return listen(c.createConsumer(c.getDestination()));
  }

  private static QueueListener directConsumer(JGroupsCluster c) throws Exception {
    return listen(c.createConsumer(c.getLocalNode().getDestination()));
  }

  private static void assertExactlyOne(String what, QueueListener q) {
    await(what, 5000, () -> q.size() >= 1);
    quietPeriod(500);
    assertEquals(what + " must be delivered exactly once", 1, q.size());
  }

  @Test
  public void broadcastIsDeliveredExactlyOncePerConsumerIncludingSender() throws Exception {
    QueueListener q1 = clusterConsumer(c1);
    QueueListener q2 = clusterConsumer(c2);
    QueueListener q3 = clusterConsumer(c3);

    c1.send(c1.getDestination(), new JGroupsMessage("hello"));

    await("all three receive", 5000, () -> q1.size() >= 1 && q2.size() >= 1 && q3.size() >= 1);
    quietPeriod(500);
    assertEquals("sender's own consumer", 1, q1.size());
    assertEquals(1, q2.size());
    assertEquals(1, q3.size());
    assertEquals("hello", q2.messages.peek().getBody(String.class));
  }

  @Test
  public void noLocalConsumerSkipsOwnBroadcastsOnly() throws Exception {
    QueueListener q = listen(c1.createConsumer(c1.getDestination(), null, true)); // noLocal
    QueueListener witness = clusterConsumer(c2);

    c1.send(c1.getDestination(), new JGroupsMessage("from-node1"));
    await("node2 sees node1's broadcast", 5000, () -> witness.size() >= 1);
    quietPeriod(500);
    assertEquals("noLocal consumer must not see its own broadcast", 0, q.size());

    c2.send(c2.getDestination(), new JGroupsMessage("from-node2"));
    assertExactlyOne("node2's broadcast on node1's noLocal consumer", q);
    assertEquals("from-node2", q.messages.peek().getBody(String.class));
  }

  @Test
  public void directMessageReachesOnlyTheTarget() throws Exception {
    QueueListener d1 = directConsumer(c1);
    QueueListener d2 = directConsumer(c2);
    QueueListener d3 = directConsumer(c3);
    QueueListener b2 = clusterConsumer(c2);

    c1.send(nodeNamed(c1, "node2").getDestination(), new JGroupsMessage("direct"));

    assertExactlyOne("direct message on node2", d2);
    assertEquals("direct", d2.messages.peek().getBody(String.class));
    assertEquals("node1 direct consumer", 0, d1.size());
    assertEquals("node3 direct consumer", 0, d3.size());
    assertEquals("node2 cluster consumer", 0, b2.size());
  }

  @Test
  public void directMessageToSelfIsDeliveredOnce() throws Exception {
    QueueListener d1 = directConsumer(c1);
    c1.send(c1.getLocalNode().getDestination(), new JGroupsMessage("self"));
    assertExactlyOne("direct message to self", d1);
  }

  @Test
  public void selectorFiltersByProperty() throws Exception {
    QueueListener q = listen(c2.createConsumer(c2.getDestination(), "kind='a'"));
    QueueListener all = clusterConsumer(c2);

    Message b = new JGroupsMessage("b");
    b.setProperty("kind", "b");
    c1.send(c1.getDestination(), b);
    Message a = new JGroupsMessage("a");
    a.setProperty("kind", "a");
    c1.send(c1.getDestination(), a);

    await("both messages reach node2", 5000, () -> all.size() >= 2);
    assertExactlyOne("kind=a message", q);
    assertEquals("a", q.messages.peek().getBody(String.class));
  }

  @Test
  public void twoConsumersOnSameDestinationBothReceive() throws Exception {
    QueueListener qa = clusterConsumer(c2);
    QueueListener qb = clusterConsumer(c2);
    c1.send(c1.getDestination(), new JGroupsMessage("x"));
    assertExactlyOne("first consumer", qa);
    assertExactlyOne("second consumer", qb);
  }

  @Test
  public void queueModeWithoutListener() throws Exception {
    MessageConsumer consumer = c2.createConsumer(c2.getDestination());
    assertNull(consumer.receiveNoWait());
    c1.send(c1.getDestination(), new JGroupsMessage("queued"));
    Message m = consumer.receive(5000);
    assertNotNull("receive(timeout) returns the message", m);
    assertEquals("queued", m.getBody(String.class));
    quietPeriod(300);
    assertNull("queue is empty again", consumer.receiveNoWait());
  }

  @Test
  public void bodiesRoundTripAcrossTheChannel() throws Exception {
    QueueListener q = clusterConsumer(c2);

    Map<String, Object> map = new HashMap<>();
    map.put("a", 1);
    map.put("b", "two");
    byte[] big = new byte[1 << 20];
    for (int i = 0; i < big.length; i++) {
      big[i] = (byte) (i * 31);
    }

    c1.send(c1.getDestination(), new JGroupsMessage("text"));
    c1.send(c1.getDestination(), new JGroupsMessage(new HashMap<>(map)));
    c1.send(c1.getDestination(), new JGroupsMessage(big));

    await("three bodies arrive", 10000, () -> q.size() >= 3);
    List<Message> got = q.list();
    assertEquals("text", got.get(0).getBody(String.class));
    assertNull("wrong type yields null", got.get(0).getBody(Integer.class));
    assertEquals(map, got.get(1).getBody(HashMap.class));
    assertArrayEquals(big, got.get(2).getBody(byte[].class));
  }

  @Test
  public void broadcastsFromOneSenderArriveInOrderWithoutLossOrDuplicates() throws Exception {
    QueueListener q = clusterConsumer(c2);
    int n = 100;
    for (int i = 0; i < n; i++) {
      Message m = new JGroupsMessage("m" + i);
      m.setProperty("seq", i);
      c1.send(c1.getDestination(), m);
    }
    await("all " + n + " arrive", 10000, () -> q.size() >= n);
    quietPeriod(500);
    List<Integer> seqs = new ArrayList<>();
    for (Message m : q.list()) {
      seqs.add((Integer) m.getProperty("seq"));
    }
    List<Integer> expected = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      expected.add(i);
    }
    assertEquals(expected, seqs);
  }

  @Test
  public void directMessageIsPointToPoint() throws Exception {
    AtomicInteger atNode3 = tap(c3).appMessages;
    QueueListener d2 = directConsumer(c2);
    c1.send(nodeNamed(c1, "node2").getDestination(), new JGroupsMessage("p2p"));
    assertExactlyOne("direct message on node2", d2);
    assertEquals("a direct message to node2 must never reach node3", 0, atNode3.get());

    // sanity check that the counter works
    c1.send(c1.getDestination(), new JGroupsMessage("broadcast"));
    await("broadcast reaches node3", 5000, () -> atNode3.get() == 1);
  }

  @Test
  public void directSendDoesNotModifyTheMessage() throws Exception {
    QueueListener d2 = directConsumer(c2);
    Message m = new JGroupsMessage("x");
    m.setProperty("k", "v");
    Map<String, Object> before = new HashMap<>(m.getProperties());
    c1.send(nodeNamed(c1, "node2").getDestination(), m);
    assertEquals("sender's message is untouched", before, m.getProperties());
    assertExactlyOne("direct", d2);
    assertEquals(
        "receiver sees exactly the sent properties", before, d2.list().get(0).getProperties());
  }

  @Test
  public void directMessageToDestinationBuiltByName() throws Exception {
    QueueListener d2 = directConsumer(c2);
    c1.send(new JGroupsDestination("node2"), new JGroupsMessage("by-name"));
    assertExactlyOne("direct by name", d2);
  }

  @Test
  public void directMessageToUnknownMemberThrows() throws Exception {
    try {
      c1.send(new JGroupsDestination("ghost"), new JGroupsMessage("x"));
      fail("sending to a node that is not a member must throw");
    } catch (ClusterException expected) {
    }
  }

  @Test
  public void directMessageToDepartedMemberThrows() throws Exception {
    JGroupsDestination dest = (JGroupsDestination) nodeNamed(c1, "node2").getDestination();
    c2.stop();
    await("node2 gone from node1", 10000, () -> nodeNamed(c1, "node2") == null);
    try {
      c1.send(dest, new JGroupsMessage("x"));
      fail("sending to a departed node must throw");
    } catch (ClusterException expected) {
    }
  }

  @Test
  public void directMessageRespectsSelector() throws Exception {
    QueueListener q = listen(c2.createConsumer(c2.getLocalNode().getDestination(), "kind='a'"));
    Message b = new JGroupsMessage("b");
    b.setProperty("kind", "b");
    Message a = new JGroupsMessage("a");
    a.setProperty("kind", "a");
    JGroupsDestination to = new JGroupsDestination("node2");
    c1.send(to, b);
    c1.send(to, a);
    assertExactlyOne("kind=a direct", q);
    assertEquals("a", q.list().get(0).getBody(String.class));
  }
}
