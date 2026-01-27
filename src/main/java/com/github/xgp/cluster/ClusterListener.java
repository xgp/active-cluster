package com.github.xgp.cluster;

import java.util.EventListener;

/**
 * Listener to events occuring on the cluster
 *
 * @version $Revision: 1.2 $
 */
public interface ClusterListener extends EventListener {

  /**
   * A new node has been added
   *
   * @param event
   */
  public void onNodeAdd(ClusterEvent event);

  /**
   * A node has updated its state
   *
   * @param event
   */
  public void onNodeUpdate(ClusterEvent event);

  /**
   * A node has been removed (a clean shutdown)
   *
   * @param event
   */
  public void onNodeRemoved(ClusterEvent event);

  /**
   * A node has failed due to process or network failure
   *
   * @param event
   */
  public void onNodeFailed(ClusterEvent event);

  /**
   * An election has occurred and a new coordinator has been selected
   *
   * @param event
   */
  public void onCoordinatorChanged(ClusterEvent event);
}
