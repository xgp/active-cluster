package com.github.xgp.cluster;

import com.github.xgp.cluster.election.ElectionStrategy;
import java.util.Map;

/**
 * A process's membership of a named cluster. From it you can see the members ({@link Node}s) and
 * their replicated state, watch membership change, publish this member's own state, send messages
 * to the whole cluster or to one member, and learn which member is the elected coordinator.
 *
 * <p>Typical use: create it from a {@link ClusterFactory}, add listeners, {@link #start()}, work,
 * {@link #stop()}. A stopped cluster can be started again and keeps its listeners.
 *
 * <p>Threading: events and messages are delivered on the implementation's own threads. Callbacks
 * should return quickly and must not call {@link #start()} or {@link #stop()}.
 *
 * @version $Revision: 1.5 $
 */
public interface Cluster extends Service {

  /**
   * The destination that addresses every member of the cluster.
   *
   * @return the destination to send messages to all members of the cluster
   */
  public Destination getDestination();

  /**
   * A snapshot of the current members, this one included, keyed by each member's destination. The
   * map does not change after it is returned; the {@link Node} values are live views of each
   * member.
   *
   * @return an unmodifiable map from node destination to node
   */
  public Map<Destination, Node> getNodes();

  /**
   * Adds a listener for membership, state and coordinator events. A listener added while the
   * cluster is running is immediately told the current coordinator.
   *
   * @param listener the listener to add; adding the same listener twice has no effect
   */
  public void addClusterListener(ClusterListener listener);

  /**
   * Removes a listener added with {@link #addClusterListener}.
   *
   * @param listener the listener to remove
   */
  public void removeClusterListener(ClusterListener listener);

  /**
   * The member running in this process, used to publish its state and to address messages to it.
   *
   * @return the Node representing this peer in the cluster
   */
  public LocalNode getLocalNode();

  /**
   * Replaces the strategy used to elect the coordinator. It takes effect at the next membership
   * change, and every member should use the same strategy or they will disagree.
   *
   * @param strategy the strategy, or null for the default
   */
  public void setElectionStrategy(ElectionStrategy strategy);

  // Messaging helper methods
  // -------------------------------------------------------------------------

  /**
   * Sends a message to every member (when {@code destination} is {@link #getDestination()}) or to
   * one member (when it is that member's {@link Node#getDestination()}).
   *
   * @param destination the cluster destination or a member's destination
   * @param message the message; it must not be modified after sending
   * @throws ClusterException if the cluster is not started, the destination is not a current
   *     member, or the message cannot be sent
   */
  public void send(Destination destination, Message message) throws ClusterException;

  /**
   * Creates a consumer of messages sent to {@code destination}, including this member's own.
   *
   * @param destination the cluster destination, or the local node's destination for direct messages
   * @return a newly created message consumer
   * @throws ClusterException if the cluster is not started or the destination is not supported
   */
  public MessageConsumer createConsumer(Destination destination) throws ClusterException;

  /**
   * Creates a consumer of messages sent to {@code destination} that match {@code selector}.
   *
   * @param destination the cluster destination, or the local node's destination for direct messages
   * @param selector a filter on message properties, or null for all messages; the supported syntax
   *     is implementation specific
   * @return a newly created message consumer
   * @throws ClusterException if the cluster is not started, or the destination or selector is not
   *     supported
   */
  public MessageConsumer createConsumer(Destination destination, String selector)
      throws ClusterException;

  /**
   * Creates a consumer of messages sent to {@code destination} that match {@code selector},
   * optionally ignoring messages sent by this member.
   *
   * @param destination the cluster destination, or the local node's destination for direct messages
   * @param selector a filter on message properties, or null for all messages
   * @param noLocal if true, messages sent by this member are not delivered to the consumer
   * @return a newly created message consumer
   * @throws ClusterException if the cluster is not started, or the destination or selector is not
   *     supported
   */
  public MessageConsumer createConsumer(Destination destination, String selector, boolean noLocal)
      throws ClusterException;
}
