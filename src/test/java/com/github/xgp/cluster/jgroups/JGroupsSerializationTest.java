package com.github.xgp.cluster.jgroups;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.github.xgp.cluster.ClusterEvent;
import com.github.xgp.cluster.Message;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import org.jgroups.Address;
import org.jgroups.util.UUID;
import org.junit.Test;

/** Pure (channel-free) checks of the wire types and the trivial selector. */
public class JGroupsSerializationTest {

  @SuppressWarnings("unchecked")
  static <T> T roundTrip(T o) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(o);
    }
    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (T) in.readObject();
    }
  }

  @Test
  public void messageRoundTrip() throws Exception {
    JGroupsMessage m = new JGroupsMessage("hello");
    m.setProperty("k", "v");
    m.setProperty("n", 42);
    JGroupsMessage copy = roundTrip(m);
    assertEquals(m.getId(), copy.getId());
    assertEquals("v", copy.getProperty("k"));
    assertEquals(42, copy.getProperty("n"));
    assertEquals("hello", copy.getBody(String.class));
    assertNull(copy.getBody(Integer.class));
  }

  @Test
  public void nodeRoundTrip() throws Exception {
    Address addr = UUID.randomUUID();
    JGroupsNode n = new JGroupsNode("node1", addr);
    Map<String, Object> state = new HashMap<>();
    state.put("a", 1);
    n.setState(state);
    n.setZone("dmz");
    n.setCoordinator(true);

    JGroupsNode copy = roundTrip(n);
    assertEquals(JGroupsNode.class, copy.getClass());
    assertEquals("node1", copy.getName());
    assertEquals(state, copy.getState());
    assertEquals("dmz", copy.getZone());
    assertTrue(copy.isCoordinator());
    assertEquals(addr, ((JGroupsDestination) copy.getDestination()).getAddress());
  }

  @Test
  public void localNodeRoundTrip() throws Exception {
    JGroupsLocalNode n = new JGroupsLocalNode("local", UUID.randomUUID());
    n.setZone("lan");
    JGroupsLocalNode copy = roundTrip(n);
    assertEquals("local", copy.getName());
    assertEquals("lan", copy.getZone());
    assertFalse(copy.isCoordinator());
  }

  @Test
  public void destinationRoundTripKeepsAddress() throws Exception {
    Address addr = UUID.randomUUID();
    JGroupsDestination d = new JGroupsDestination("node1", addr);
    JGroupsDestination copy = roundTrip(d);
    assertEquals("node1", copy.getName());
    assertEquals(addr, copy.getAddress());
  }

  @Test
  public void destinationRoundTripWithNullAddress() throws Exception {
    JGroupsDestination copy = roundTrip(new JGroupsDestination("cluster"));
    assertEquals("cluster", copy.getName());
    assertNull(copy.getAddress());
  }

  @Test
  public void clusterEventRoundTrip() throws Exception {
    JGroupsNode n = new JGroupsNode("node2", UUID.randomUUID());
    ClusterEvent e = new ClusterEvent(null, n, ClusterEvent.UPDATE_NODE);
    ClusterEvent copy = roundTrip(e);
    assertEquals(ClusterEvent.UPDATE_NODE, copy.getType());
    assertEquals("node2", copy.getNode().getName());
  }

  @Test
  public void clusterEventNamesEveryType() {
    JGroupsNode n = new JGroupsNode("x");
    assertTrue(new ClusterEvent(null, n, ClusterEvent.ADD_NODE).toString().contains("ADD_NODE"));
    assertTrue(
        new ClusterEvent(null, n, ClusterEvent.UPDATE_NODE).toString().contains("UPDATE_NODE"));
    assertTrue(
        new ClusterEvent(null, n, ClusterEvent.REMOVE_NODE).toString().contains("REMOVE_NODE"));
    assertTrue(
        new ClusterEvent(null, n, ClusterEvent.FAILED_NODE).toString().contains("FAILED_NODE"));
    assertTrue(
        new ClusterEvent(null, n, ClusterEvent.ELECTED_COORDINATOR)
            .toString()
            .contains("ELECTED_COORDINATOR"));
  }

  @Test
  public void destinationEqualityIsByNameOnly() {
    JGroupsDestination a = new JGroupsDestination("n", UUID.randomUUID());
    JGroupsDestination b = new JGroupsDestination("n", UUID.randomUUID());
    JGroupsDestination c = new JGroupsDestination("n");
    assertEquals(a, b);
    assertEquals(a, c);
    assertEquals(a.hashCode(), b.hashCode());
    assertEquals(a.hashCode(), c.hashCode());
    assertNotEquals(a, new JGroupsDestination("m"));
  }

  @Test
  public void largeBodyRoundTrip() throws Exception {
    byte[] big = new byte[1 << 20];
    for (int i = 0; i < big.length; i++) {
      big[i] = (byte) i;
    }
    JGroupsMessage copy = roundTrip(new JGroupsMessage(big));
    assertArrayEquals(big, copy.getBody(byte[].class));
  }

  // Selector
  // -------------------------------------------------------------------------

  private static boolean matches(String selector, String key, Object value) {
    JGroupsMessageConsumer consumer =
        new JGroupsMessageConsumer(
            new JGroupsCluster("n", "c", null, null), new JGroupsDestination("c"), selector, false);
    Message m = new JGroupsMessage("x");
    if (key != null) {
      m.setProperty(key, value);
    }
    return consumer.matchesSelector(m);
  }

  @Test
  public void nullSelectorMatchesAll() {
    assertTrue(matches(null, null, null));
    assertTrue(matches(null, "k", "v"));
  }

  @Test
  public void quotedAndUnquotedSelectorsMatch() {
    assertTrue(matches("k='v'", "k", "v"));
    assertTrue(matches("k=v", "k", "v"));
    assertTrue(matches(" k = 'v' ", "k", "v"));
  }

  @Test
  public void selectorMismatchAndMissingPropertyDoNotMatch() {
    assertFalse(matches("k='v'", "k", "w"));
    assertFalse(matches("k='v'", "other", "v"));
    assertFalse(matches("k='v'", null, null));
  }
}
