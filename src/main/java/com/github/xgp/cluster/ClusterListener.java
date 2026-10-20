package com.github.xgp.cluster;

import java.util.EventListener;

/**
 * Receives membership, state and coordinator events. Each event's {@link ClusterEvent#getNode()} is
 * the member it concerns; for remote members it is the same object found in {@link
 * Cluster#getNodes()}.
 *
 * <p>Events are delivered in order on an implementation thread. Implementations should return
 * quickly; an exception thrown by one listener does not affect the others.
 *
 * @version $Revision: 1.2 $
 */
public interface ClusterListener extends EventListener {

  /**
   * A member joined (or returned after a partition healed). Not fired for the local member.
   *
   * @param event the event; its node is the new member
   */
  public void onNodeAdd(ClusterEvent event);

  /**
   * A remote member published a changed state or zone.
   *
   * @param event the event; its node already carries the new state
   */
  public void onNodeUpdate(ClusterEvent event);

  /**
   * A member left. Fired alone for a clean {@link Cluster#stop()}, and after {@link #onNodeFailed}
   * when the member crashed or became unreachable.
   *
   * @param event the event; its node is the departed member
   */
  public void onNodeRemoved(ClusterEvent event);

  /**
   * A member left without stopping cleanly: it crashed or is unreachable (for example on the far
   * side of a network partition). {@link #onNodeRemoved} follows.
   *
   * @param event the event; its node is the failed member
   */
  public void onNodeFailed(ClusterEvent event);

  /**
   * A different member is now the coordinator. Also delivered to a listener added while the cluster
   * is running, naming the current coordinator.
   *
   * @param event the event; its node is the new coordinator
   */
  public void onCoordinatorChanged(ClusterEvent event);
}
