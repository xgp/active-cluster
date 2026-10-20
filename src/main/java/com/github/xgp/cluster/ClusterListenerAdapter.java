package com.github.xgp.cluster;

/**
 * A {@link ClusterListener} whose methods do nothing, for subclasses that handle only some events.
 */
public abstract class ClusterListenerAdapter implements ClusterListener {

  @Override
  public void onNodeAdd(ClusterEvent event) {}

  @Override
  public void onNodeUpdate(ClusterEvent event) {}

  @Override
  public void onNodeRemoved(ClusterEvent event) {}

  @Override
  public void onNodeFailed(ClusterEvent event) {}

  @Override
  public void onCoordinatorChanged(ClusterEvent event) {}
}
