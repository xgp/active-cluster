package com.github.xgp.cluster.jgroups;

import static org.junit.Assert.fail;

import com.github.xgp.cluster.Cluster;
import com.github.xgp.cluster.ClusterEvent;
import com.github.xgp.cluster.ClusterListener;
import com.github.xgp.cluster.Message;
import com.github.xgp.cluster.MessageListener;
import com.github.xgp.cluster.Node;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.MergeView;
import org.jgroups.Receiver;
import org.jgroups.View;
import org.jgroups.protocols.DISCARD;
import org.jgroups.util.Util;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TestName;

/**
 * Shared fixture for cluster tests: starts clusters on a per-test-method cluster name, stops them
 * all afterwards, and provides recording listeners plus polling waits.
 */
public abstract class TestSupport {

  public static final String LOOPBACK_CONFIG = "test-loopback.xml";
  public static final String TCP_CONFIG = "test-tcp.xml";
  public static final String PARTITION_CONFIG = "test-partition.xml";

  @Rule public final TestName testName = new TestName();

  private final List<Cluster> clusters = new ArrayList<>();
  private String clusterName;

  /** The JGroups stack used by {@link #startCluster}. Overridden by the TCP tier. */
  protected String config() {
    return LOOPBACK_CONFIG;
  }

  /** A cluster name unique to the current test method so loopback state cannot bleed across. */
  protected synchronized String clusterName() {
    if (clusterName == null) {
      clusterName = "t-" + testName.getMethodName() + "-" + System.nanoTime();
    }
    return clusterName;
  }

  /** Creates (without starting) a cluster member that {@link #stopAll} will clean up. */
  protected JGroupsCluster createCluster(String nodeName) throws Exception {
    JGroupsCluster c =
        (JGroupsCluster) new JGroupsClusterFactory(config()).createCluster(nodeName, clusterName());
    synchronized (clusters) {
      clusters.add(c);
    }
    return c;
  }

  /** Creates and starts a cluster member. */
  protected JGroupsCluster startCluster(String nodeName) throws Exception {
    JGroupsCluster c = createCluster(nodeName);
    c.start();
    return c;
  }

  /** Creates a member, attaches the listener before start, then starts it. */
  protected JGroupsCluster startCluster(String nodeName, ClusterListener listener)
      throws Exception {
    JGroupsCluster c = createCluster(nodeName);
    c.addClusterListener(listener);
    c.start();
    return c;
  }

  @After
  public void stopAll() {
    List<Cluster> toStop;
    synchronized (clusters) {
      toStop = new ArrayList<>(clusters);
      clusters.clear();
    }
    Collections.reverse(toStop);
    for (Cluster c : toStop) {
      try {
        c.stop();
      } catch (Exception e) {
        // best effort; a crashed member may already be closed
      }
    }
  }

  // Waiting
  // -------------------------------------------------------------------------

  /** Polls {@code cond} every 50 ms until true, failing with {@code what} after the timeout. */
  public static void await(String what, long timeoutMs, BooleanSupplier cond) {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    while (true) {
      try {
        if (cond.getAsBoolean()) {
          return;
        }
      } catch (RuntimeException e) {
        // condition not evaluable yet; keep polling
      }
      if (System.nanoTime() > deadline) {
        fail("Timed out after " + timeoutMs + " ms waiting for: " + what);
      }
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        fail("Interrupted waiting for: " + what);
      }
    }
  }

  /** Fixed pause, only for asserting that something does NOT happen. */
  public static void quietPeriod(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** Waits until all channels have installed the same view. */
  public static void awaitSameView(Cluster... cs) throws Exception {
    JChannel[] channels = new JChannel[cs.length];
    for (int i = 0; i < cs.length; i++) {
      channels[i] = ((JGroupsCluster) cs[i]).getChannel();
    }
    Util.waitUntilAllChannelsHaveSameView(10000, 100, channels);
  }

  /** Waits until every cluster's {@code getNodes()} names equal exactly {@code names}. */
  public static void awaitNodeNames(Set<String> names, Cluster... cs) {
    for (Cluster c : cs) {
      await(
          c.getLocalNode().getName() + " sees nodes " + names,
          10000,
          () -> nodeNames(c).equals(names));
    }
  }

  /** Waits for the full view and for every member's node map to reflect it. */
  public static void awaitConverged(Cluster... cs) throws Exception {
    awaitSameView(cs);
    Set<String> names = new TreeSet<>();
    for (Cluster c : cs) {
      names.add(c.getLocalNode().getName());
    }
    awaitNodeNames(names, cs);
  }

  public static Set<String> nodeNames(Cluster c) {
    Set<String> names = new TreeSet<>();
    for (Node n : c.getNodes().values()) {
      names.add(n.getName());
    }
    return names;
  }

  public static Node nodeNamed(Cluster c, String name) {
    for (Node n : c.getNodes().values()) {
      if (name.equals(n.getName())) {
        return n;
      }
    }
    return null;
  }

  /** Names of nodes this member currently flags as coordinator. */
  public static List<String> coordinators(Cluster c) {
    List<String> result = new ArrayList<>();
    for (Node n : c.getNodes().values()) {
      if (n.isCoordinator()) {
        result.add(n.getName());
      }
    }
    return result;
  }

  /** Kills a member's channel without a graceful leave so failure detection must notice. */
  public static void crash(Cluster c) throws Exception {
    Util.shutdown(((JGroupsCluster) c).getChannel());
  }

  // Interception and partitions
  // -------------------------------------------------------------------------

  /** What a {@link #tap} observed on one member's channel. */
  public static class Tap {
    public final AtomicInteger appMessages = new AtomicInteger();
    public final AtomicInteger mergeViews = new AtomicInteger();
  }

  /**
   * Interposes on a member's JGroups receiver, counting application messages that physically reach
   * the channel and merge views it installs, then delegating everything to the cluster.
   */
  public static Tap tap(JGroupsCluster c) {
    JChannel ch = c.getChannel();
    Receiver target = ch.getReceiver();
    Tap tap = new Tap();
    ch.setReceiver(
        new Receiver() {
          public void receive(org.jgroups.Message msg) {
            if (msg.getObject() instanceof com.github.xgp.cluster.Message) {
              tap.appMessages.incrementAndGet();
            }
            target.receive(msg);
          }

          public void viewAccepted(View view) {
            if (view instanceof MergeView) {
              tap.mergeViews.incrementAndGet();
            }
            target.viewAccepted(view);
          }

          public void getState(OutputStream out) throws Exception {
            target.getState(out);
          }

          public void setState(InputStream in) throws Exception {
            target.setState(in);
          }
        });
    return tap;
  }

  private static DISCARD discard(Cluster c) {
    DISCARD d = ((JGroupsCluster) c).getChannel().getProtocolStack().findProtocol(DISCARD.class);
    if (d == null) {
      throw new IllegalStateException("stack has no DISCARD protocol; use test-partition.xml");
    }
    return d;
  }

  /** Cuts every member of {@code a} off from every member of {@code b}, in both directions. */
  public static void partition(Cluster[] a, Cluster[] b) {
    for (Cluster x : a) {
      for (Cluster y : b) {
        Address ax = ((JGroupsCluster) x).getChannel().getAddress();
        Address ay = ((JGroupsCluster) y).getChannel().getAddress();
        discard(x).addIgnoreMember(ay);
        discard(y).addIgnoreMember(ax);
      }
    }
  }

  /** Removes every partition installed by {@link #partition}. */
  public static void heal(Cluster... cs) {
    for (Cluster c : cs) {
      discard(c).resetIgnoredMembers();
    }
  }

  /** Waits until each member's own channel view has exactly {@code size} members. */
  public static void awaitViewSize(int size, long timeoutMs, Cluster... cs) {
    for (Cluster c : cs) {
      await(
          c.getLocalNode().getName() + " view size " + size,
          timeoutMs,
          () -> ((JGroupsCluster) c).getChannel().getView().size() == size);
    }
  }

  // Recorders
  // -------------------------------------------------------------------------

  /** Records cluster events as {@code "TYPE:nodeName"}. */
  public static class RecordingListener implements ClusterListener {
    public static final String ADD = "ADD_NODE";
    public static final String UPDATE = "UPDATE_NODE";
    public static final String REMOVE = "REMOVE_NODE";
    public static final String FAILED = "FAILED_NODE";
    public static final String COORD = "ELECTED_COORDINATOR";

    public final List<String> events = new CopyOnWriteArrayList<>();

    private void record(String type, ClusterEvent e) {
      events.add(type + ":" + e.getNode().getName());
    }

    public void onNodeAdd(ClusterEvent e) {
      record(ADD, e);
    }

    public void onNodeUpdate(ClusterEvent e) {
      record(UPDATE, e);
    }

    public void onNodeRemoved(ClusterEvent e) {
      record(REMOVE, e);
    }

    public void onNodeFailed(ClusterEvent e) {
      record(FAILED, e);
    }

    public void onCoordinatorChanged(ClusterEvent e) {
      record(COORD, e);
    }

    public int count(String type, String nodeName) {
      String key = type + ":" + nodeName;
      int n = 0;
      for (String s : events) {
        if (s.equals(key)) {
          n++;
        }
      }
      return n;
    }

    public int count(String type) {
      int n = 0;
      for (String s : events) {
        if (s.startsWith(type + ":")) {
          n++;
        }
      }
      return n;
    }

    public int indexOf(String type, String nodeName) {
      return events.indexOf(type + ":" + nodeName);
    }

    public void clear() {
      events.clear();
    }

    @Override
    public String toString() {
      return events.toString();
    }
  }

  /** Collects delivered messages in a queue. */
  public static class QueueListener implements MessageListener {
    public final LinkedBlockingQueue<Message> messages = new LinkedBlockingQueue<>();

    public void onMessage(Message message) {
      messages.add(message);
    }

    public int size() {
      return messages.size();
    }

    public List<Message> list() {
      return new ArrayList<>(messages);
    }
  }
}
