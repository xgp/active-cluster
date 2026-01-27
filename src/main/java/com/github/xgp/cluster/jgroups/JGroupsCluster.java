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
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.MergeView;
import org.jgroups.Receiver;
import org.jgroups.View;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.NameCache;

/**
 * JGroups implementation of the Cluster interface.
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

  private final String clusterName;
  private final JGroupsDestinationMarshaller marshaller;
  private final JGroupsLocalNode localNode;
  private final JGroupsDestination clusterDestination;
  private final String jgroupsConfig;
  private final Map<Destination, Node> nodes = Collections.synchronizedMap(new HashMap<>());
  private final List<ClusterListener> clusterListeners = new CopyOnWriteArrayList<>();
  private final List<JGroupsMessageConsumer> messageConsumers = new CopyOnWriteArrayList<>();

  private JChannel channel;
  private RpcDispatcher dispatcher;
  private ElectionStrategy electionStrategy;
  private Node coordinator;

  // Status flags
  private boolean started = false;
  private boolean coordinator_elected = false;

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
  public void start() throws ClusterException {
    if (started) {
      return;
    }

    try {
      // Initialize the JGroups channel - try to find the configuration
      String configFile = jgroupsConfig != null ? jgroupsConfig : DEFAULT_JGROUPS_CONFIG;

      try {
        // Try to find the specified config
        channel = new JChannel(configFile);
        LOG.info("Using JGroups config: " + configFile);
      } catch (Exception e) {
        LOG.log(
            Level.WARNING,
            "Failed to load JGroups config: " + configFile + ", trying alternate config",
            e);

        // First try with default JGroups config (no file)
        try {
          channel = new JChannel();
          LOG.info("Using JGroups built-in default configuration");
        } catch (Exception e2) {
          LOG.log(Level.WARNING, "Failed to use default config, trying other config files", e2);

          // Try different configs in a fallback sequence
          String[] configs =
              new String[] {DEFAULT_JGROUPS_CONFIG, SIMPLE_CONFIG, UDP_CONFIG, TCP_CONFIG};

          boolean success = false;
          for (String cfg : configs) {
            if (!cfg.equals(configFile)) { // Skip the one we already tried
              try {
                channel = new JChannel(cfg);
                LOG.info("Successfully loaded config: " + cfg);
                success = true;
                break;
              } catch (Exception e3) {
                LOG.log(Level.FINE, "Failed to load config: " + cfg, e3);
              }
            }
          }

          if (!success) {
            LOG.log(
                Level.SEVERE,
                "Failed to load any JGroups configuration, creating with system defaults");
            channel = new JChannel(false); // Create without any config
          }
        }
      }

      // Name the channel for easier debugging
      channel.setName(localNode.getName());
      channel.setReceiver(this);

      // RPC dispatcher is optional; direct messaging is sufficient for this implementation.
      dispatcher = null;

      // Add local node to the nodes map
      nodes.put(localNode.getDestination(), localNode);

      // Connect to the cluster
      channel.connect(clusterName);

      // Get the state after connecting
      try {
        // Request state transfer after connecting
        channel.getState(null, 10000);
        LOG.info("Requested state from existing members");
      } catch (Exception e) {
        LOG.log(Level.WARNING, "Failed to get initial state: " + e.getMessage());
        // Continue anyway, since this might be the first node
      }

      // Set the address in the destination
      if (localNode.getDestination() instanceof JGroupsDestination) {
        ((JGroupsDestination) localNode.getDestination()).setAddress(channel.getAddress());
      }

      // Mark as started
      started = true;

      // Important: Broadcast our presence to the cluster
      broadcastNodeState();

      // Log info about the cluster
      LOG.info("Node " + localNode.getName() + " connected to cluster " + clusterName);
      LOG.info("Channel view: " + channel.getView());

      // Initialize coordinator if we're the first node
      if (channel.getView().size() == 1) {
        coordinator = localNode;
        localNode.setCoordinator(true);
        fireCoordinatorChanged();
        LOG.info("Node " + localNode.getName() + " is the coordinator");
      } else {
        LOG.info("Cluster size: " + channel.getView().size() + " members");
      }
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Error starting cluster", e);
      throw new ClusterException();
    }
  }

  @Override
  public void stop() throws ClusterException {
    if (!started) {
      return;
    }

    try {
      // Clean up all message consumers
      for (JGroupsMessageConsumer consumer : new ArrayList<>(messageConsumers)) {
        try {
          consumer.close();
        } catch (Exception e) {
          LOG.log(Level.WARNING, "Error closing consumer", e);
        }
      }

      // Disconnect and close the channel
      if (channel != null) {
        channel.disconnect();
        channel.close();
        channel = null;
      }

      // Clean up
      nodes.clear();
      clusterListeners.clear();
      messageConsumers.clear();
      coordinator = null;

      // Mark as stopped
      started = false;
    } catch (Exception e) {
      throw new ClusterException();
    }
  }

  @Override
  public Destination getDestination() {
    return clusterDestination;
  }

  @Override
  public Map<Destination, Node> getNodes() {
    return Collections.unmodifiableMap(nodes);
  }

  @Override
  public void addClusterListener(ClusterListener listener) {
    if (listener != null && !clusterListeners.contains(listener)) {
      clusterListeners.add(listener);
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
      LOG.info("Sending message to destination: " + destination);

      if (destination.equals(clusterDestination)) {
        // Send to entire cluster
        org.jgroups.Message jgMsg = new org.jgroups.ObjectMessage(null, message);

        // For reliable delivery, set these flags
        jgMsg.setFlag(org.jgroups.Message.Flag.RSVP); // Request ACK
        jgMsg.setFlag(org.jgroups.Message.Flag.NO_FC); // No flow control

        LOG.info("Sending to all cluster members via: " + channel.getView());
        LOG.info(
            "Sending cluster message details: dest="
                + jgMsg.getDest()
                + ", flags="
                + jgMsg.getFlags());
        channel.send(jgMsg);

        // Also process the message locally to ensure the sender gets it too
        for (JGroupsMessageConsumer consumer : messageConsumers) {
          if (consumer.getDestination().equals(clusterDestination)
              && !consumer.isNoLocal()
              && consumer.matchesSelector(message)) {
            try {
              consumer.onMessage(message);
            } catch (Exception e) {
              LOG.warning("Error delivering message locally: " + e.getMessage());
            }
          }
        }
      } else if (destination instanceof JGroupsDestination) {
        // Send to specific node
        JGroupsDestination jgDest = (JGroupsDestination) destination;

        // Send as a targeted cluster message to avoid unreliable unicast delivery.
        message.setProperty("__direct_to", jgDest.getName());
        org.jgroups.Message jgMsg = new org.jgroups.ObjectMessage(null, message);

        // For reliable delivery, set these flags
        jgMsg.setFlag(org.jgroups.Message.Flag.RSVP); // Request ACK
        jgMsg.setFlag(org.jgroups.Message.Flag.NO_FC); // No flow control

        LOG.info("Sending targeted message to node: " + jgDest.getName());
        LOG.info(
            "Targeted message details: dest="
                + jgMsg.getDest()
                + ", flags="
                + jgMsg.getFlags()
                + ", __direct_to="
                + message.getProperty("__direct_to"));
        channel.send(jgMsg);
      } else {
        LOG.warning("Unknown destination type: " + destination.getClass().getName());
        throw new ClusterException();
      }
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

  /** Broadcast the local node's state to the cluster. */
  public void broadcastNodeState() throws Exception {
    if (!started || channel == null) {
      throw new ClusterException();
    }

    // First update the local node address if needed
    if (localNode.getDestination() instanceof JGroupsDestination) {
      JGroupsDestination dest = (JGroupsDestination) localNode.getDestination();
      if (dest.getAddress() == null) {
        dest.setAddress(channel.getAddress());
      }
    }

    LOG.info("Broadcasting node state for " + localNode.getName());

    // Method 1: Use the RPC dispatcher (if configured)
    if (dispatcher != null) {
      try {
        ClusterEvent event = new ClusterEvent(this, localNode, ClusterEvent.UPDATE_NODE);
        dispatcher.callRemoteMethods(
            null,
            "handleNodeUpdate",
            new Object[] {event},
            new Class<?>[] {ClusterEvent.class},
            new RequestOptions(ResponseMode.GET_NONE, 5000));
        LOG.info("Sent state via RPC");
      } catch (Exception e) {
        LOG.warning("RPC state broadcast failed: " + e.getMessage());
      }
    }

    // Method 2: Direct messaging as backup
    try {
      ClusterEvent event = new ClusterEvent(this, localNode, ClusterEvent.UPDATE_NODE);
      org.jgroups.Message jgMsg = new org.jgroups.BytesMessage();
      jgMsg.setObject(event);
      channel.send(jgMsg);
      LOG.info("Sent state via direct message");

      // Also handle it locally to ensure consistency
      handleNodeUpdate(event);
    } catch (Exception e) {
      LOG.warning("Direct message state broadcast failed: " + e.getMessage());
      throw e;
    }
  }

  /**
   * Handle a node update event from a remote node.
   *
   * @param event the cluster event containing the updated node
   */
  public void handleNodeUpdate(ClusterEvent event) {
    if (event != null && event.getNode() != null) {
      Node node = event.getNode();
      event.setCluster(this);

      // Remove any stale placeholder entries that share the same address but different name
      if (node.getDestination() instanceof JGroupsDestination) {
        JGroupsDestination incomingDest = (JGroupsDestination) node.getDestination();
        Address incomingAddr = incomingDest.getAddress();
        if (incomingAddr != null) {
          for (Map.Entry<Destination, Node> entry : new ArrayList<>(nodes.entrySet())) {
            if (entry.getKey() instanceof JGroupsDestination) {
              JGroupsDestination existingDest = (JGroupsDestination) entry.getKey();
              if (incomingAddr.equals(existingDest.getAddress())
                  && !existingDest.equals(incomingDest)) {
                nodes.remove(entry.getKey());
              }
            }
          }
        }
      }

      // Update our local nodes map
      nodes.put(node.getDestination(), node);

      // Notify listeners
      for (ClusterListener listener : clusterListeners) {
        listener.onNodeUpdate(event);
      }
    }
  }

  /** Fire a coordinator changed event to all listeners. */
  private void fireCoordinatorChanged() {
    if (coordinator != null) {
      ClusterEvent event = new ClusterEvent(this, coordinator, ClusterEvent.ELECTED_COORDINATOR);
      for (ClusterListener listener : clusterListeners) {
        listener.onCoordinatorChanged(event);
      }
    }
  }

  /** Trigger a new coordinator election. */
  private void electCoordinator() {
    if (!started || electionStrategy == null) {
      return;
    }

    try {
      Node electedNode = electionStrategy.doElection(this);

      if (electedNode != null) {
        // Update coordinator flag on all nodes
        for (Node node : nodes.values()) {
          if (node instanceof JGroupsNode) {
            ((JGroupsNode) node).setCoordinator(node.equals(electedNode));
          }
        }

        this.coordinator = electedNode;
        fireCoordinatorChanged();
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
      LOG.info(
          "receive() invoked: src="
              + jgMsg.getSrc()
              + ", dest="
              + jgMsg.getDest()
              + ", flags="
              + jgMsg.getFlags()
              + ", class="
              + jgMsg.getClass().getName());
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
        LOG.info("Received application message from " + sender);

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

            if (consumer.isNoLocal() && Objects.equals(channel.getAddress(), sender)) {
              continue;
            }

            if (consumer.matchesSelector(message)) {
              consumer.onMessage(message);
              delivered = true;
            }
          }

          if (!delivered) {
            LOG.warning("Direct message had no matching consumers, might be dropped: " + message);
          }

          return;
        }

        // Deliver to all appropriate consumers
        boolean delivered = false;
        for (JGroupsMessageConsumer consumer : messageConsumers) {
          // Skip if noLocal is set and this is a local message
          if (consumer.isNoLocal() && Objects.equals(channel.getAddress(), sender)) {
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
          LOG.warning("Message had no matching consumers, might be dropped: " + message);
        }
      } else if (obj instanceof ClusterEvent) {
        // Handle cluster events directly
        ClusterEvent event = (ClusterEvent) obj;
        LOG.info("Received cluster event: " + event.getType());

        if (event.getType() == ClusterEvent.UPDATE_NODE) {
          updateNodeAddressFromMessage(event, jgMsg);
          handleNodeUpdate(event);
        }
      } else {
        LOG.warning(
            "Received unrecognized message type: "
                + (obj != null ? obj.getClass().getName() : "null"));
      }
    } catch (Exception e) {
      LOG.log(Level.WARNING, "Error processing received message", e);
      e.printStackTrace();
    }
  }

  @Override
  public void viewAccepted(View view) {
    if (!started) {
      return;
    }

    try {
      LOG.info("View members: " + view.getMembers());
      LOG.info("NameCache: " + NameCache.printCache());
      LOG.info("View change received: " + view);

      if (view instanceof MergeView) {
        // Handle cluster merge - need to reconcile state
        LOG.info("Cluster merge detected: " + view);
        handleClusterMerge((MergeView) view);
      } else {
        // Handle regular view change
        handleViewChange(view);
      }

      // Elect a coordinator if needed
      if (coordinator == null || !nodes.containsValue(coordinator)) {
        electCoordinator();
      }

      // After a view change, broadcast our state to make sure everyone knows about us
      try {
        broadcastNodeState();
        LOG.info("Broadcast node state after view change");
      } catch (Exception e) {
        LOG.warning("Failed to broadcast node state after view change: " + e.getMessage());
      }

      // Log current cluster status
      LOG.info("Current cluster has " + nodes.size() + " nodes after view change");
      for (Node node : nodes.values()) {
        LOG.info("  - Node: " + node.getName() + (node.isCoordinator() ? " (coordinator)" : ""));
      }
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Error handling view change", e);
    }
  }

  public void suspect(Address suspectedAddress) {
    // Handle suspected node failure
    if (!started || suspectedAddress == null) {
      return;
    }

    try {
      // Find the node that might have failed
      Node failedNode = null;
      Destination failedDest = null;

      for (Map.Entry<Destination, Node> entry : nodes.entrySet()) {
        if (entry.getKey() instanceof JGroupsDestination) {
          JGroupsDestination dest = (JGroupsDestination) entry.getKey();
          if (dest.getAddress() != null && dest.getAddress().equals(suspectedAddress)) {
            failedNode = entry.getValue();
            failedDest = entry.getKey();
            break;
          }
        }
      }

      if (failedNode != null && failedDest != null) {
        // Notify listeners of suspected failure
        ClusterEvent event = new ClusterEvent(this, failedNode, ClusterEvent.FAILED_NODE);
        for (ClusterListener listener : clusterListeners) {
          listener.onNodeFailed(event);
        }
      }
    } catch (Exception e) {
      LOG.log(Level.WARNING, "Error handling suspect notification", e);
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
   * Handle a JGroups view change event.
   *
   * @param view the new cluster view
   */
  private void handleViewChange(View view) {
    if (view == null) {
      return;
    }

    List<Address> members = view.getMembers();
    List<Address> oldMembers = new ArrayList<>();

    // Get current member addresses
    for (Node node : nodes.values()) {
      if (node instanceof JGroupsNode) {
        JGroupsDestination dest = (JGroupsDestination) node.getDestination();
        if (dest.getAddress() != null) {
          oldMembers.add(dest.getAddress());
        }
      }
    }

    // Check for nodes that have left
    for (Address oldAddr : oldMembers) {
      if (!members.contains(oldAddr)) {
        // Find the node that left
        Node leftNode = null;
        Destination leftDest = null;

        for (Map.Entry<Destination, Node> entry : nodes.entrySet()) {
          JGroupsDestination dest = (JGroupsDestination) entry.getKey();
          if (dest.getAddress() != null && dest.getAddress().equals(oldAddr)) {
            leftNode = entry.getValue();
            leftDest = entry.getKey();
            break;
          }
        }

        if (leftNode != null && leftDest != null) {
          // Remove from nodes map
          nodes.remove(leftDest);

          // Notify listeners
          ClusterEvent event = new ClusterEvent(this, leftNode, ClusterEvent.REMOVE_NODE);
          for (ClusterListener listener : clusterListeners) {
            listener.onNodeRemoved(event);
          }

          // Reset coordinator if it was this node
          if (leftNode.equals(coordinator)) {
            coordinator = null;
          }
        }
      }
    }

    // Check for new nodes - these would have joined through JChannel.connect()
    // and should be added to our view
    for (Address addr : members) {
      if (addr.equals(channel.getAddress())) {
        continue;
      }

      String name = NameCache.get(addr);
      if (name == null || name.isEmpty()) {
        name = addr.toString();
      }

      // If we already have a node with this name but no address, fill it in.
      JGroupsNode existing = findNodeByName(name);
      if (existing != null) {
        if (existing.getDestination() instanceof JGroupsDestination) {
          JGroupsDestination existingDest = (JGroupsDestination) existing.getDestination();
          if (existingDest.getAddress() == null) {
            existingDest.setAddress(addr);
            LOG.info("Updated node address from view: " + name + " (" + addr + ")");
          }
        }
        continue;
      }

      boolean found = false;
      for (Destination dest : nodes.keySet()) {
        if (dest instanceof JGroupsDestination) {
          JGroupsDestination jgDest = (JGroupsDestination) dest;
          if (jgDest.getAddress() != null && jgDest.getAddress().equals(addr)) {
            found = true;
            break;
          }
        }
      }

      if (!found) {
        // Create a placeholder node from the view so we can surface it immediately
        JGroupsNode node = new JGroupsNode(name, addr);
        nodes.put(node.getDestination(), node);

        ClusterEvent event = new ClusterEvent(this, node, ClusterEvent.ADD_NODE);
        for (ClusterListener listener : clusterListeners) {
          listener.onNodeAdd(event);
        }

        LOG.info("New node detected and added from view: " + name + " (" + addr + ")");
      }
    }
  }

  /**
   * Handle a JGroups merge view event (when a network partition heals).
   *
   * @param mergeView the merge view
   */
  private void handleClusterMerge(MergeView mergeView) {
    LOG.info("Handling cluster merge: " + mergeView);

    // For a merge, we need to request full state from all subgroups
    // This is a simplistic implementation - in production, you might want
    // more sophisticated conflict resolution

    // Reset coordinator since we need to elect a new one across the merged cluster
    coordinator = null;

    // Ask for state transfer to synchronize node information
    try {
      channel.getState(null, 10000);
    } catch (Exception e) {
      LOG.log(Level.WARNING, "Error requesting state during merge", e);
    }
  }

  @Override
  public void getState(OutputStream output) throws Exception {
    // Instead of using Util.objectToStream which requires DataOutput,
    // we'll create our own serialization approach that's more reliable
    synchronized (nodes) {
      try {
        // We'll serialize the nodes manually to have more control
        java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(output);

        // Write the number of nodes actually serialized
        Collection<Node> nodeList = nodes.values();
        List<Node> toWrite = new ArrayList<>();
        for (Node node : nodeList) {
          if (node instanceof JGroupsLocalNode) {
            toWrite.add(copyAsRemoteNode(node));
          } else if (node instanceof JGroupsNode) {
            toWrite.add(node);
          }
        }

        oos.writeInt(toWrite.size());

        // Write each node individually
        for (Node node : toWrite) {
          oos.writeObject(node);
        }

        oos.flush();
        LOG.info("Serialized " + toWrite.size() + " nodes for state transfer");
      } catch (Exception e) {
        LOG.log(Level.SEVERE, "Error serializing state", e);
        throw e;
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void setState(InputStream input) throws Exception {
    // Use a more careful deserialization approach
    try {
      java.io.ObjectInputStream ois = new java.io.ObjectInputStream(input);

      // Read the node count
      int nodeCount = ois.readInt();
      LOG.info("Preparing to receive " + nodeCount + " nodes from state transfer");

      // Update our local view
      synchronized (nodes) {
        // Keep our local node
        Node localNode = nodes.get(this.localNode.getDestination());
        if (localNode == null) {
          nodes.put(this.localNode.getDestination(), this.localNode);
          localNode = this.localNode;
        }

        // Snapshot existing destinations for add events
        Map<Destination, Node> existing = new HashMap<>(nodes);

        // Read each node
        for (int i = 0; i < nodeCount; i++) {
          try {
            Node node = (Node) ois.readObject();
            if (node != null && !node.equals(localNode)) {
              Node prior = nodes.get(node.getDestination());
              if (prior != null
                  && prior.getDestination() instanceof JGroupsDestination
                  && node.getDestination() instanceof JGroupsDestination) {
                JGroupsDestination priorDest = (JGroupsDestination) prior.getDestination();
                JGroupsDestination incomingDest = (JGroupsDestination) node.getDestination();
                if (incomingDest.getAddress() == null && priorDest.getAddress() != null) {
                  incomingDest.setAddress(priorDest.getAddress());
                }
              }

              boolean isNew = !existing.containsKey(node.getDestination());
              nodes.put(node.getDestination(), node);

              if (isNew) {
                ClusterEvent event = new ClusterEvent(this, node, ClusterEvent.ADD_NODE);
                for (ClusterListener listener : clusterListeners) {
                  listener.onNodeAdd(event);
                }
              }
            }
          } catch (Exception e) {
            LOG.log(Level.WARNING, "Error deserializing a node, skipping", e);
            // Continue with next node
          }
        }

        LOG.info("State transfer completed, cluster has " + nodes.size() + " nodes");
      }
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Error deserializing state", e);
      throw e;
    }
  }

  // JGroups RequestHandler implementation

  @Override
  public Object handle(org.jgroups.Message msg) throws Exception {
    if (msg == null) {
      return null;
    }

    LOG.info(
        "handle() invoked: src="
            + msg.getSrc()
            + ", dest="
            + msg.getDest()
            + ", flags="
            + msg.getFlags()
            + ", class="
            + msg.getClass().getName());
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

  private void updateNodeAddressFromMessage(ClusterEvent event, org.jgroups.Message jgMsg) {
    if (event == null || event.getNode() == null || jgMsg == null) {
      return;
    }

    if (event.getNode().getDestination() instanceof JGroupsDestination) {
      JGroupsDestination dest = (JGroupsDestination) event.getNode().getDestination();
      if (dest.getAddress() == null) {
        dest.setAddress(jgMsg.getSrc());
      }
    }
  }

  private JGroupsNode findNodeByName(String name) {
    if (name == null) {
      return null;
    }

    for (Node node : nodes.values()) {
      if (node instanceof JGroupsNode && name.equals(node.getName())) {
        return (JGroupsNode) node;
      }
    }

    return null;
  }

  private JGroupsNode copyAsRemoteNode(Node node) {
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

  private Address resolveAddressByName(String name) {
    if (name == null || channel == null) {
      return null;
    }

    View view = channel.getView();
    if (view == null) {
      return null;
    }

    for (Address addr : view.getMembers()) {
      String addrName = NameCache.get(addr);
      if (name.equals(addrName)) {
        return addr;
      }
    }

    return null;
  }
}
