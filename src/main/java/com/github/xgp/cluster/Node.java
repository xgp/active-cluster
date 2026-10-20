package com.github.xgp.cluster;

import java.util.Map;

/**
 * A member of a cluster.
 *
 * @version $Revision: 1.3 $
 */
public interface Node {

  /**
   * The destination that addresses this member alone.
   *
   * @return the destination to send messages to this node while it is a member
   */
  public Destination getDestination();

  /**
   * @return an immutable snapshot of the node's most recently published state
   */
  public Map<String, Object> getState();

  /**
   * @return the name of the node, unique within the cluster
   */
  public String getName();

  /**
   * @return true if this node is the currently elected coordinator
   */
  public boolean isCoordinator();

  /**
   * The zone of this node, typically the DMZ or subnet it runs in.
   *
   * @return the zone, or null if none was set
   */
  public Object getZone();
}
