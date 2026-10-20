package com.github.xgp.cluster.group;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.github.xgp.cluster.ClusterEvent;
import com.github.xgp.cluster.Node;
import com.github.xgp.cluster.jgroups.JGroupsNode;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

/** Pure tests of the in-memory group and buddy models. */
public class GroupModelTest {

  private static Node node(String name) {
    return new JGroupsNode(name);
  }

  private static Node node(String name, String zone) {
    JGroupsNode n = new JGroupsNode(name);
    n.setZone(zone);
    return n;
  }

  private static GroupModel model() {
    GroupModel m = new GroupModel();
    m.setMinimumMemberCount(2);
    m.setMaximumMemberCount(3);
    return m;
  }

  private static Group only(GroupModel m) {
    List<?> groups = m.getGroups();
    assertEquals(1, groups.size());
    return (Group) groups.get(0);
  }

  @Test
  public void groupFillsFromUnusableToUsableToFull() {
    GroupModel m = model();
    Node n1 = node("n1"), n2 = node("n2"), n3 = node("n3");

    m.addNode(n1);
    Group g = only(m);
    assertFalse(g.isUsable());
    assertFalse(g.isFull());

    m.addNode(n2);
    assertSame(g, only(m));
    assertTrue(g.isUsable());
    assertFalse(g.isFull());

    m.addNode(n3);
    assertSame(g, only(m));
    assertTrue(g.isFull());
    assertEquals(Arrays.asList(n1, n2, n3), g.getMembers());
  }

  @Test
  public void fourthNodeStartsNewGroup() {
    GroupModel m = model();
    for (String n : new String[] {"n1", "n2", "n3", "n4"}) {
      m.addNode(node(n));
    }
    List<?> groups = m.getGroups();
    assertEquals(2, groups.size());
    Group second = (Group) groups.get(1);
    assertEquals(1, second.getMemberCount());
    assertFalse(second.isUsable());
  }

  @Test
  public void removeNodeMovesGroupBack() {
    GroupModel m = model();
    Node n1 = node("n1"), n2 = node("n2"), n3 = node("n3");
    m.addNode(n1);
    m.addNode(n2);
    m.addNode(n3);
    Group g = only(m);
    assertTrue(g.isFull());

    m.removeNode(n3);
    assertFalse(g.isFull());
    assertTrue(g.isUsable());

    m.removeNode(n2);
    assertFalse(g.isUsable());

    // the incomplete group is refilled before any new group is created
    Node n4 = node("n4");
    m.addNode(n4);
    assertSame(g, only(m));
    assertTrue(g.isUsable());
    assertTrue(g.getMembers().contains(n4));
  }

  @Test
  public void masterZoneFilterSendsNonMatchingNodesToUnusedPool() {
    GroupModel m = model();
    m.setMasterFilter(new MasterZoneFilter(Arrays.asList("dmz")));

    m.addNode(node("lan1", "lan"));
    assertTrue("a non-dmz node must not become a master", m.getGroups().isEmpty());

    Node master = node("dmz1", "dmz");
    m.addNode(master);
    Group g = only(m);
    assertEquals(master, g.getMembers().get(0));
  }

  @Test
  public void buddyModelGivesEachNodeItsOwnGroupWithBuddies() {
    BuddyGroupModel m = new BuddyGroupModel();
    m.setMinimumMemberCount(2);
    m.setMaximumMemberCount(3);
    Node n1 = node("n1"), n2 = node("n2");

    m.addNode(n1);
    assertEquals(1, m.getGroups().size());

    m.addNode(n2);
    List<?> groups = m.getGroups();
    assertEquals(2, groups.size());
    Group g1 = (Group) groups.get(0);
    Group g2 = (Group) groups.get(1);
    assertEquals(n1, g1.getMembers().get(0));
    assertEquals(n2, g2.getMembers().get(0));
    assertTrue(g1.getMembers().contains(n2));
    assertTrue(g2.getMembers().contains(n1));
    assertTrue(g1.isUsable());
    assertTrue(g2.isUsable());
  }

  @Test
  public void groupClusterListenerForwardsMembershipEvents() {
    GroupModel m = model();
    GroupClusterListener l = new GroupClusterListener(m);
    Node n1 = node("n1"), n2 = node("n2"), n3 = node("n3");

    l.onNodeAdd(new ClusterEvent(null, n1, ClusterEvent.ADD_NODE));
    l.onNodeAdd(new ClusterEvent(null, n2, ClusterEvent.ADD_NODE));
    l.onNodeAdd(new ClusterEvent(null, n3, ClusterEvent.ADD_NODE));
    Group g = only(m);
    assertEquals(3, g.getMemberCount());

    l.onNodeUpdate(new ClusterEvent(null, n1, ClusterEvent.UPDATE_NODE));
    assertEquals(3, g.getMemberCount());

    l.onNodeRemoved(new ClusterEvent(null, n3, ClusterEvent.REMOVE_NODE));
    assertEquals(2, g.getMemberCount());

    l.onNodeFailed(new ClusterEvent(null, n2, ClusterEvent.FAILED_NODE));
    assertEquals(1, g.getMemberCount());
    assertEquals(Arrays.asList(n1), g.getMembers());
  }
}
