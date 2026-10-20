package com.github.xgp.cluster.jgroups;

import com.github.xgp.cluster.Cluster;
import com.github.xgp.cluster.ClusterEvent;
import com.github.xgp.cluster.ClusterException;
import com.github.xgp.cluster.ClusterListener;
import com.github.xgp.cluster.Destination;
import com.github.xgp.cluster.LocalNode;
import com.github.xgp.cluster.Message;
import com.github.xgp.cluster.MessageConsumer;
import com.github.xgp.cluster.Node;
import com.github.xgp.cluster.election.ElectionStrategy;
import com.github.xgp.cluster.election.impl.BullyElectionStrategy;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.MergeView;
import org.jgroups.Receiver;
import org.jgroups.View;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.util.NameCache;

/**
 * JGroups implementation of the Cluster interface.
 *
 * <p>Membership follows the JGroups view: a remote member is added to {@link #getNodes()} (firing
 * {@code ADD_NODE}) as soon as it appears in a view and its logical name is known, and removed when
 * a view drops it. A member that left via {@link #stop()} announces it first and fires only {@code
 * REMOVE_NODE} on the survivors; any other departure fires {@code FAILED_NODE} then {@code
 * REMOVE_NODE}.
 *
 * <p>Each node object in {@link #getNodes()} keeps its identity for as long as the member stays in
 * the cluster. State broadcasts update remote entries in place, and {@code UPDATE_NODE} fires only
 * when a remote member's state or zone actually changed. A coordinator is elected on every
 * membership change and {@code ELECTED_COORDINATOR} fires only when the winner changes.
 *
 * @version $Revision: 1.0 $
 */
public class JGroupsCluster implements Cluster, Receiver, RequestHandler {

  private static final Logger LOG = Logger.getLogger(JGroupsCluster.class.getName());

  // Default JGroups configuration if none provided
  private static final String DEFAULT_JGROUPS_CONFIG = "minimal.xml";
  private static final String SIMPLE_CONFIG = "simple-udp.xml";
  private static final String TCP_CONFIG = "tcp.xml";
  private static final String UDP_CONFIG = "udp.xml";

  /** How long {@link #stop()} waits for survivors to acknowledge the leave announcement. */
  private static final long LEAVE_ACK_TIMEOUT_MS = 3000;

  /** How long {@link #start()} waits for the state transfer from existing members. */
  private static final long STATE_TRANSFER_TIMEOUT_MS = 10000;

  private final String clusterName;
  private final JGroupsDestinationMarshaller marshaller;
  private final JGroupsLocalNode localNode;
  private final JGroupsDestination clusterDestination;
  private final String jgroupsConfig;

  /**
   * Members keyed by destination (equal by node name). Its monitor guards every field below that is
   * documented as "guarded by nodes", and cluster events are fired while holding it so listeners
   * observe them in a consistent order.
   */
  private final Map<Destination, Node> nodes = Collections.synchronizedMap(new LinkedHashMap<>());

  private final List<ClusterListener> clusterListeners = new CopyOnWriteArrayList<>();
  private final List<JGroupsMessageConsumer> messageConsumers = new CopyOnWriteArrayList<>();

  /** Last view applied to {@link #nodes}. Guarded by nodes. */
  private View currentView;

  /** Name of the currently elected coordinator. Guarded by nodes. */
  private String coordinatorName;

  /** Members that announced a clean leave and are still in the current view. Guarded by nodes. */
  private final Set<Address> leaving = new HashSet<>();

  /**
   * State broadcasts from members that are not (yet) in the current view, or whose name could not
   * be resolved when their view arrived. Applied when the view catches up. Guarded by nodes.
   */
  private final Map<Address, JGroupsNode> pending = new HashMap<>();

  /** Members whose leave acknowledgement {@link #stop()} is waiting for. */
  private final Set<Address> awaitingLeaveAcks = Collections.synchronizedSet(new HashSet<>());

  private volatile CountDownLatch leaveAcks;
  private volatile JChannel channel;
  private volatile ElectionStrategy electionStrategy;
  private volatile boolean started = false;

  /**
   * Create a new JGroups cluster with the provided name and configuration.
   *
   * @param localNodeName name of the local node
   * @param clusterName name of the cluster
   * @param configFile JGroups XML configuration file (can be null for default)
   * @param marshaller destination marshaller (can be null for default)
   */
  public JGroupsCluster(
      String localNodeName,
      String clusterName,
      String configFile,
      JGroupsDestinationMarshaller marshaller) {

    this.clusterName = clusterName;
    this.jgroupsConfig = configFile;
    this.marshaller = marshaller != null ? marshaller : new JGroupsDestinationMarshaller();
    this.clusterDestination = new JGroupsDestination(clusterName);
    this.localNode = new JGroupsLocalNode(localNodeName);
    this.localNode.setCluster(this);
    this.electionStrategy = new BullyElectionStrategy();
  }

  /**
   * Get the JGroups channel used by this cluster.
   *
   * @return the JGroups channel
   */
  public JChannel getChannel() {
    return channel;
  }

  @Override
  public synchronized void start() throws ClusterException {
    if (started) {
      return;
    }

    try {
      channel = createChannel();

      // Name the channel for easier debugging; the logical name is also how peers learn ours
      channel.setName(localNode.getName());
      channel.setReceiver(this);

      synchronized (nodes) {
        nodes.clear();
        nodes.put(localNode.getDestination(), localNode);
        currentView = null;
        coordinatorName = null;
        leaving.clear();
        pending.clear();
        localNode.setCoordinator(false);
      }

      // Started before connect so the first view, delivered during connect, is processed.
      started = true;
      channel.connect(clusterName);
      localDestination().setAddress(channel.getAddress());

      // Pull the existing members' node map (names, addresses and state)
      View view = channel.getView();
      if (view != null && view.size() > 1) {
        try {
          channel.getState(null, STATE_TRANSFER_TIMEOUT_MS);
        } catch (Exception e) {
          LOG.log(Level.WARNING, "Failed to get initial state: " + e.getMessage());
        }
      }

      // In case the connect-time view callback was missed, apply the current view (idempotent)
      handleView(channel.getView());

      // Tell the cluster who we are and what our state is
      broadcastNodeState();

      LOG.info(
          "Node "
              + localNode.getName()
              + " connected to cluster "
              + clusterName
              + ", view: "
              + channel.getView());
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Error starting cluster", e);
      started = false;
      closeChannel();
      throw new ClusterException();
    }
  }

  private JChannel createChannel() throws Exception {
    // A config the caller named explicitly must load as given: silently substituting a
    // different stack (e.g. UDP for a broken TCP config) would join the wrong cluster.
    if (jgroupsConfig != null) {
      JChannel ch = new JChannel(jgroupsConfig);
      LOG.info("Using JGroups config: " + jgroupsConfig);
      return ch;
    }

    // No config named: try the default, then fall back through the bundled configs
    String configFile = DEFAULT_JGROUPS_CONFIG;

    try {
      // Try to find the specified config
      JChannel ch = new JChannel(configFile);
      LOG.info("Using JGroups config: " + configFile);
      return ch;
    } catch (Exception e) {
      LOG.log(
          Level.WARNING,
          "Failed to load JGroups config: " + configFile + ", trying alternate config",
          e);

      // First try with default JGroups config (no file)
      try {
        JChannel ch = new JChannel();
        LOG.info("Using JGroups built-in default configuration");
        return ch;
      } catch (Exception e2) {
        LOG.log(Level.WARNING, "Failed to use default config, trying other config files", e2);

        // Try different configs in a fallback sequence
        String[] configs =
            new String[] {DEFAULT_JGROUPS_CONFIG, SIMPLE_CONFIG, UDP_CONFIG, TCP_CONFIG};

        for (String cfg : configs) {
          if (!cfg.equals(configFile)) { // Skip the one we already tried
            try {
              JChannel ch = new JChannel(cfg);
              LOG.info("Successfully loaded config: " + cfg);
              return ch;
            } catch (Exception e3) {
              LOG.log(Level.FINE, "Failed to load config: " + cfg, e3);
            }
          }
        }

        LOG.log(
            Level.SEVERE,
            "Failed to load any JGroups configuration, creating with system defaults");
        return new JChannel(false); // Create without any config
      }
    }
  }

  @Override
  public synchronized void stop() throws ClusterException {
    if (!started) {
      return;
    }

    // Clean up all message consumers
    for (JGroupsMessageConsumer consumer : new ArrayList<>(messageConsumers)) {
      try {
        consumer.close();
      } catch (Exception e) {
        LOG.log(Level.WARNING, "Error closing consumer", e);
      }
    }
    messageConsumers.clear();

    announceLeave();

    // Ignore any callbacks from here on; listeners are kept so a restart keeps them.
    started = false;
    closeChannel();

    synchronized (nodes) {
      nodes.clear();
      currentView = null;
      coordinatorName = null;
      leaving.clear();
      pending.clear();
      localNode.setCoordinator(false);
    }
  }

  /**
   * Tells the other members this is a clean leave and waits (bounded) until each has acknowledged,
   * so the view that drops us is reported as REMOVE_NODE rather than a failure.
   */
  private void announceLeave() {
    JChannel ch = channel;
    if (ch == null || !ch.isConnected()) {
      return;
    }
    View view = ch.getView();
    if (view == null || view.size() <= 1) {
      return;
    }
    try {
      Set<Address> others = new HashSet<>(view.getMembers());
      others.remove(ch.getAddress());
      awaitingLeaveAcks.clear();
      awaitingLeaveAcks.addAll(others);
      CountDownLatch latch = new CountDownLatch(others.size());
      leaveAcks = latch;
      ch.send(
          new org.jgroups.ObjectMessage(
              null, new ControlMessage(ControlMessage.Kind.LEAVING, localNode.getName())));
      if (!latch.await(LEAVE_ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        LOG.warning("Leave not acknowledged by " + awaitingLeaveAcks + "; leaving anyway");
      }
    } catch (Exception e) {
      LOG.log(Level.FINE, "Could not announce leave", e);
    } finally {
      leaveAcks = null;
      awaitingLeaveAcks.clear();
    }
  }

  private void closeChannel() {
    JChannel ch = channel;
    channel = null;
    if (ch != null) {
      try {
        ch.disconnect();
      } catch (Exception e) {
        LOG.log(Level.FINE, "Error disconnecting channel", e);
      }
      try {
        ch.close();
      } catch (Exception e) {
        LOG.log(Level.FINE, "Error closing channel", e);
      }
    }
  }

  @Override
  public Destination getDestination() {
    return clusterDestination;
  }

  /** A snapshot of the current members; the node objects themselves are live. */
  @Override
  public Map<Destination, Node> getNodes() {
    synchronized (nodes) {
      return Collections.unmodifiableMap(new LinkedHashMap<>(nodes));
    }
  }

  /**
   * Adds a listener. If the cluster is already started and a coordinator is known, the listener is
   * immediately sent an {@code ELECTED_COORDINATOR} event for it, so a listener added after {@link
   * #start()} still learns the current coordinator.
   */
  @Override
  public void addClusterListener(ClusterListener listener) {
    if (listener == null) {
      return;
    }
    synchronized (nodes) {
      if (clusterListeners.contains(listener)) {
        return;
      }
      clusterListeners.add(listener);
      if (started && coordinatorName != null) {
        Node coordinator = nodes.get(new JGroupsDestination(coordinatorName));
        if (coordinator != null) {
          try {
            listener.onCoordinatorChanged(
                new ClusterEvent(this, coordinator, ClusterEvent.ELECTED_COORDINATOR));
          } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "Cluster listener failed", e);
          }
        }
      }
    }
  }

  @Override
  public void removeClusterListener(ClusterListener listener) {
    clusterListeners.remove(listener);
  }

  @Override
  public LocalNode getLocalNode() {
    return localNode;
  }

  @Override
  public void setElectionStrategy(ElectionStrategy strategy) {
    this.electionStrategy = strategy != null ? strategy : new BullyElectionStrategy();
  }

  @Override
  public void send(Destination destination, Message message) throws ClusterException {
    if (!started) {
      throw new ClusterException();
    }

    try {
      LOG.fine("Sending message to destination: " + destination);

      if (destination.equals(clusterDestination)) {
        // Send to entire cluster
        org.jgroups.Message jgMsg = new org.jgroups.ObjectMessage(null, message);

        // For reliable delivery, set these flags
        jgMsg.setFlag(org.jgroups.Message.Flag.RSVP); // Request ACK
        jgMsg.setFlag(org.jgroups.Message.Flag.NO_FC); // No flow control

        // JGroups loops the multicast back into receive(), which delivers it to local
        // consumers (honouring noLocal), so there is no separate local delivery here.
        channel.send(jgMsg);
      } else if (destination instanceof JGroupsDestination) {
        // Send to specific node
        JGroupsDestination jgDest = (JGroupsDestination) destination;

        // Send as a targeted cluster message to avoid unreliable unicast delivery.
        message.setProperty("__direct_to", jgDest.getName());
        org.jgroups.Message jgMsg = new org.jgroups.ObjectMessage(null, message);

        // For reliable delivery, set these flags
        jgMsg.setFlag(org.jgroups.Message.Flag.RSVP); // Request ACK
        jgMsg.setFlag(org.jgroups.Message.Flag.NO_FC); // No flow control

        LOG.fine("Sending targeted message to node: " + jgDest.getName());
        channel.send(jgMsg);
      } else {
        LOG.warning("Unknown destination type: " + destination.getClass().getName());
        throw new ClusterException();
      }
    } catch (ClusterException e) {
      throw e;
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Error sending message", e);
      throw new ClusterException();
    }
  }

  @Override
  public MessageConsumer createConsumer(Destination destination) throws ClusterException {
    return createConsumer(destination, null, false);
  }

  @Override
  public MessageConsumer createConsumer(Destination destination, String selector)
      throws ClusterException {
    return createConsumer(destination, selector, false);
  }

  @Override
  public MessageConsumer createConsumer(Destination destination, String selector, boolean noLocal)
      throws ClusterException {

    if (!started) {
      throw new ClusterException();
    }

    if (!(destination instanceof JGroupsDestination)) {
      throw new ClusterException();
    }

    JGroupsDestination jgDest = (JGroupsDestination) destination;
    JGroupsMessageConsumer consumer = new JGroupsMessageConsumer(this, jgDest, selector, noLocal);
    return consumer;
  }

  /**
   * Register a message consumer with this cluster.
   *
   * @param consumer the consumer to register
   */
  public void registerConsumer(JGroupsMessageConsumer consumer) {
    if (consumer != null && !messageConsumers.contains(consumer)) {
      messageConsumers.add(consumer);
    }
  }

  /**
   * Unregister a message consumer from this cluster.
   *
   * @param consumer the consumer to unregister
   */
  public void unregisterConsumer(JGroupsMessageConsumer consumer) {
    messageConsumers.remove(consumer);
  }

  /**
   * Broadcast the local node's state to the other members. A detached copy is sent, never the local
   * node itself, so receivers only ever hold plain {@link JGroupsNode}s for remote members.
   */
  public void broadcastNodeState() throws Exception {
    JChannel ch = channel;
    if (!started || ch == null) {
      throw new ClusterException();
    }

    JGroupsDestination dest = localDestination();
    if (dest.getAddress() == null) {
      dest.setAddress(ch.getAddress());
    }

    LOG.fine("Broadcasting node state for " + localNode.getName());

    ClusterEvent event =
        new ClusterEvent(this, copyAsRemoteNode(localNode), ClusterEvent.UPDATE_NODE);
    org.jgroups.Message jgMsg = new org.jgroups.BytesMessage();
    jgMsg.setObject(event);
    jgMsg.setFlag(org.jgroups.Message.TransientFlag.DONT_LOOPBACK);
    ch.send(jgMsg);
  }

  /**
   * Apply a remote member's state broadcast. Ignored for the local node. An existing entry is
   * updated in place (keeping its identity and coordinator flag) and {@code UPDATE_NODE} fires only
   * if its state or zone changed.
   *
   * @param event the cluster event containing the updated node
   * @param src the JGroups address the broadcast came from
   */
  void handleNodeUpdate(ClusterEvent event, Address src) {
    if (event == null || !(event.getNode() instanceof JGroupsNode) || src == null) {
      return;
    }
    JGroupsNode incoming = (JGroupsNode) event.getNode();
    JChannel ch = channel;
    if (incoming.getName() == null
        || incoming.getName().equals(localNode.getName())
        || (ch != null && src.equals(ch.getAddress()))) {
      return;
    }

    synchronized (nodes) {
      if (!started) {
        return;
      }
      if (currentView == null || !currentView.containsMember(src)) {
        // Not in our view yet: remember it and apply it when the view arrives
        pending.put(src, incoming);
        return;
      }
      Node existing = nodes.get(incoming.getDestination());
      if (existing == null) {
        // A view member whose name we could not resolve when its view arrived
        addRemote(incoming.getName(), src, incoming);
        elect();
        return;
      }
      updateRemote((JGroupsNode) existing, src, incoming);
    }
  }

  /** Creates and registers a remote member, firing ADD_NODE. Caller holds the nodes lock. */
  private JGroupsNode addRemote(String name, Address addr, Node info) {
    JGroupsNode node = new JGroupsNode(name, addr);
    if (info != null) {
      applyInfo(node, info);
    }
    nodes.put(node.getDestination(), node);
    LOG.info("Node " + localNode.getName() + " added member " + name + " (" + addr + ")");
    fire(new ClusterEvent(this, node, ClusterEvent.ADD_NODE));
    return node;
  }

  /** Updates a remote member in place, firing UPDATE_NODE if it changed. Caller holds the lock. */
  private void updateRemote(JGroupsNode existing, Address addr, Node info) {
    if (existing == localNode) {
      return;
    }
    JGroupsDestination dest = (JGroupsDestination) existing.getDestination();
    if (addr != null && !addr.equals(dest.getAddress())) {
      dest.setAddress(addr);
    }
    if (applyInfo(existing, info)) {
      fire(new ClusterEvent(this, existing, ClusterEvent.UPDATE_NODE));
    }
  }

  /** Copies state and zone (never the coordinator flag); returns true if anything changed. */
  private static boolean applyInfo(JGroupsNode target, Node info) {
    boolean changed = false;
    Map<String, Object> state = info.getState();
    if (!Objects.equals(target.getState(), state)) {
      try {
        target.setState(state);
      } catch (ClusterException e) {
        // JGroupsNode.setState does not throw
      }
      changed = true;
    }
    if (!Objects.equals(target.getZone(), info.getZone())) {
      target.setZone(info.getZone());
      changed = true;
    }
    return changed;
  }

  /** Delivers an event to every listener. Caller holds the nodes lock. */
  private void fire(ClusterEvent event) {
    for (ClusterListener listener : clusterListeners) {
      try {
        switch (event.getType()) {
          case ClusterEvent.ADD_NODE:
            listener.onNodeAdd(event);
            break;
          case ClusterEvent.UPDATE_NODE:
            listener.onNodeUpdate(event);
            break;
          case ClusterEvent.REMOVE_NODE:
            listener.onNodeRemoved(event);
            break;
          case ClusterEvent.FAILED_NODE:
            listener.onNodeFailed(event);
            break;
          case ClusterEvent.ELECTED_COORDINATOR:
            listener.onCoordinatorChanged(event);
            break;
          default:
            break;
        }
      } catch (RuntimeException e) {
        LOG.log(Level.WARNING, "Cluster listener failed on " + event, e);
      }
    }
  }

  /**
   * Runs the election strategy over the current members, flags the winner on every node object and
   * fires ELECTED_COORDINATOR if the winner changed. Caller holds the nodes lock.
   */
  private void elect() {
    ElectionStrategy strategy = electionStrategy;
    if (!started || strategy == null) {
      return;
    }

    try {
      Node elected = strategy.doElection(this);
      if (elected == null) {
        return;
      }
      String winner = elected.getName();
      for (Node node : nodes.values()) {
        if (node instanceof JGroupsNode) {
          ((JGroupsNode) node).setCoordinator(winner.equals(node.getName()));
        }
      }
      if (!winner.equals(coordinatorName)) {
        coordinatorName = winner;
        Node coordinator = nodes.get(new JGroupsDestination(winner));
        LOG.info("Node " + localNode.getName() + " elected coordinator " + winner);
        fire(
            new ClusterEvent(
                this,
                coordinator != null ? coordinator : elected,
                ClusterEvent.ELECTED_COORDINATOR));
      }
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Error during coordinator election", e);
    }
  }

  // JGroups Receiver interface methods

  @Override
  public void receive(org.jgroups.Message jgMsg) {
    if (!started || jgMsg == null) {
      return;
    }

    try {
      Object obj = jgMsg.getObject();
      LOG.fine(
          "Received message from "
              + jgMsg.getSrc()
              + ", object type: "
              + (obj != null ? obj.getClass().getName() : "null"));

      if (obj instanceof Message) {
        Message message = (Message) obj;

        // Get the sender's address
        Address sender = jgMsg.getSrc();
        JChannel ch = channel;
        Address self = ch != null ? ch.getAddress() : null;

        // Handle targeted direct messages first
        Object directTarget = message.getProperty("__direct_to");
        if (directTarget instanceof String) {
          String targetName = (String) directTarget;
          if (!localNode.getName().equals(targetName)) {
            return; // Not for this node
          }

          boolean delivered = false;
          for (JGroupsMessageConsumer consumer : messageConsumers) {
            if (!consumer.getDestination().equals(localNode.getDestination())) {
              continue;
            }

            if (consumer.isNoLocal() && Objects.equals(self, sender)) {
              continue;
            }

            if (consumer.matchesSelector(message)) {
              consumer.onMessage(message);
              delivered = true;
            }
          }

          if (!delivered) {
            LOG.fine("Direct message had no matching consumers: " + message);
          }

          return;
        }

        // Deliver to all appropriate consumers
        boolean delivered = false;
        for (JGroupsMessageConsumer consumer : messageConsumers) {
          // Skip if noLocal is set and this is a local message
          if (consumer.isNoLocal() && Objects.equals(self, sender)) {
            continue;
          }

          // Check if the message matches this consumer's destination
          boolean destMatch = matchesConsumerDestination(consumer, jgMsg);

          if (destMatch && consumer.matchesSelector(message)) {
            consumer.onMessage(message);
            delivered = true;
          }
        }

        if (!delivered) {
          LOG.fine("Message had no matching consumers: " + message);
        }
      } else if (obj instanceof ClusterEvent) {
        ClusterEvent event = (ClusterEvent) obj;
        if (event.getType() == ClusterEvent.UPDATE_NODE) {
          handleNodeUpdate(event, jgMsg.getSrc());
        }
      } else if (obj instanceof ControlMessage) {
        handleControl((ControlMessage) obj, jgMsg.getSrc());
      } else {
        LOG.warning(
            "Received unrecognized message type: "
                + (obj != null ? obj.getClass().getName() : "null"));
      }
    } catch (Exception e) {
      LOG.log(Level.WARNING, "Error processing received message", e);
    }
  }

  private void handleControl(ControlMessage control, Address src) throws Exception {
    JChannel ch = channel;
    if (src == null || ch == null) {
      return;
    }
    switch (control.getKind()) {
      case LEAVING:
        if (src.equals(ch.getAddress())) {
          return;
        }
        synchronized (nodes) {
          leaving.add(src);
        }
        ch.send(
            new org.jgroups.ObjectMessage(
                src, new ControlMessage(ControlMessage.Kind.LEAVE_ACK, localNode.getName())));
        break;
      case LEAVE_ACK:
        CountDownLatch latch = leaveAcks;
        if (latch != null && awaitingLeaveAcks.remove(src)) {
          latch.countDown();
        }
        break;
      default:
        break;
    }
  }

  @Override
  public void viewAccepted(View view) {
    if (!started) {
      return;
    }

    try {
      if (view instanceof MergeView) {
        LOG.info("Cluster merge detected: " + view);
      }
      handleView(view);
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Error handling view change", e);
    }
  }

  @Override
  public void block() {
    // Called before a possible state transfer - nothing to do
  }

  @Override
  public void unblock() {
    // Called after state transfer is done - nothing to do
  }

  /**
   * Reconciles {@link #nodes} with a view: departed members are removed (REMOVE_NODE, preceded by
   * FAILED_NODE unless they announced a clean leave), new members whose names are known are added
   * (ADD_NODE), then a coordinator is elected. Views older than the last one applied are ignored,
   * which makes this safe to call more than once for the same view.
   */
  private void handleView(View view) {
    if (view == null) {
      return;
    }

    boolean membersAdded = false;
    synchronized (nodes) {
      if (!started) {
        return;
      }
      if (currentView != null && view.getViewId().compareToIDs(currentView.getViewId()) <= 0) {
        return;
      }
      currentView = view;
      LOG.fine("Node " + localNode.getName() + " applying view " + view);

      JChannel ch = channel;
      Address self = ch != null ? ch.getAddress() : null;
      JGroupsDestination localDest = localDestination();
      if (self != null && localDest.getAddress() == null) {
        localDest.setAddress(self);
      }

      List<Address> members = view.getMembers();

      // Departures
      for (Node node : new ArrayList<>(nodes.values())) {
        if (node == localNode) {
          continue;
        }
        Address addr = ((JGroupsDestination) node.getDestination()).getAddress();
        if (addr != null && members.contains(addr)) {
          continue;
        }
        nodes.remove(node.getDestination());
        boolean clean = addr != null && leaving.remove(addr);
        LOG.info(
            "Node "
                + localNode.getName()
                + (clean ? " saw member leave: " : " saw member fail: ")
                + node.getName());
        if (!clean) {
          fire(new ClusterEvent(this, node, ClusterEvent.FAILED_NODE));
        }
        fire(new ClusterEvent(this, node, ClusterEvent.REMOVE_NODE));
      }

      // Arrivals
      for (Address addr : members) {
        if (addr.equals(self) || findNodeByAddress(addr) != null) {
          continue;
        }
        JGroupsNode info = pending.remove(addr);
        String name = info != null ? info.getName() : NameCache.get(addr);
        if (name == null || name.isEmpty()) {
          // Added when its state broadcast (which carries its name) arrives
          LOG.fine("No logical name yet for " + addr);
          continue;
        }
        if (name.equals(localNode.getName())) {
          LOG.warning("Ignoring member " + addr + " with the same name as the local node");
          continue;
        }
        Node sameName = nodes.get(new JGroupsDestination(name));
        if (sameName instanceof JGroupsNode) {
          // Same name, new address: a restarted member whose old incarnation is still listed
          updateRemote((JGroupsNode) sameName, addr, info != null ? info : sameName);
          continue;
        }
        addRemote(name, addr, info);
        membersAdded = true;
      }

      pending.keySet().retainAll(members);
      leaving.retainAll(members);

      elect();
    }

    // Make sure newcomers have our current state even if the state transfer raced with an update.
    // Receivers ignore unchanged state, so this never produces spurious UPDATE_NODE events.
    if (membersAdded) {
      try {
        broadcastNodeState();
      } catch (Exception e) {
        LOG.log(Level.FINE, "Failed to broadcast node state after view change", e);
      }
    }
  }

  @Override
  public void getState(OutputStream output) throws Exception {
    List<JGroupsNode> toWrite = new ArrayList<>();
    synchronized (nodes) {
      for (Node node : nodes.values()) {
        toWrite.add(copyAsRemoteNode(node));
      }
    }
    ObjectOutputStream oos = new ObjectOutputStream(output);
    oos.writeInt(toWrite.size());
    for (Node node : toWrite) {
      oos.writeObject(node);
    }
    oos.flush();
    LOG.fine("Serialized " + toWrite.size() + " nodes for state transfer");
  }

  @Override
  public void setState(InputStream input) throws Exception {
    ObjectInputStream ois = new ObjectInputStream(input);
    int nodeCount = ois.readInt();
    List<JGroupsNode> received = new ArrayList<>();
    for (int i = 0; i < nodeCount; i++) {
      Object o = ois.readObject();
      if (o instanceof JGroupsNode) {
        received.add((JGroupsNode) o);
      }
    }

    synchronized (nodes) {
      if (!started) {
        return;
      }
      boolean added = false;
      for (JGroupsNode node : received) {
        if (node.getName() == null || node.getName().equals(localNode.getName())) {
          continue;
        }
        Address addr = ((JGroupsDestination) node.getDestination()).getAddress();
        if (addr == null) {
          continue;
        }
        if (currentView == null || !currentView.containsMember(addr)) {
          pending.put(addr, node);
          continue;
        }
        Node existing = nodes.get(node.getDestination());
        if (existing instanceof JGroupsNode) {
          updateRemote((JGroupsNode) existing, addr, node);
        } else if (findNodeByAddress(addr) == null) {
          addRemote(node.getName(), addr, node);
          added = true;
        }
      }
      if (added) {
        elect();
      }
      LOG.fine("State transfer completed, cluster has " + nodes.size() + " nodes");
    }
  }

  // JGroups RequestHandler implementation

  @Override
  public Object handle(org.jgroups.Message msg) throws Exception {
    if (msg == null) {
      return null;
    }

    Object obj = msg.getObject();
    if (obj instanceof Message) {
      // If a direct message is routed through the request handler, handle it here.
      receive(msg);
    }

    return null; // No RPC responses used in this implementation
  }

  private boolean matchesConsumerDestination(
      JGroupsMessageConsumer consumer, org.jgroups.Message jgMsg) {
    if (consumer == null || jgMsg == null) {
      return false;
    }

    JGroupsDestination consumerDest = consumer.getDestination();
    if (consumerDest == null) {
      return false;
    }

    // Multicast messages are for the cluster destination
    if (jgMsg.getDest() == null) {
      return consumerDest.equals(clusterDestination);
    }

    // Unicast messages are for the local node destination
    return consumerDest.equals(localNode.getDestination());
  }

  private JGroupsDestination localDestination() {
    return (JGroupsDestination) localNode.getDestination();
  }

  /** Caller holds the nodes lock. */
  private Node findNodeByAddress(Address addr) {
    for (Node node : nodes.values()) {
      if (addr.equals(((JGroupsDestination) node.getDestination()).getAddress())) {
        return node;
      }
    }
    return null;
  }

  private static JGroupsNode copyAsRemoteNode(Node node) {
    JGroupsNode copy = new JGroupsNode(node.getName());
    try {
      copy.setState(node.getState());
    } catch (ClusterException e) {
      // Ignore state copy failures, keep empty state.
    }
    copy.setZone(node.getZone());
    copy.setCoordinator(node.isCoordinator());

    if (node.getDestination() instanceof JGroupsDestination
        && copy.getDestination() instanceof JGroupsDestination) {
      JGroupsDestination from = (JGroupsDestination) node.getDestination();
      JGroupsDestination to = (JGroupsDestination) copy.getDestination();
      to.setAddress(from.getAddress());
    }

    return copy;
  }
}
