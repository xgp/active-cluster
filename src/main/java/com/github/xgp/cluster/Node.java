package com.github.xgp.cluster;

import java.util.Map;

/**
 * Represents a node member in a cluster
 *
 * @version $Revision: 1.3 $
 */
public interface Node {

  /**
   * Access to the queue to send messages direct to this node.
   *
   * @return the destination to send messages to this node while its available
   */
  public Destination getDestination();

  /**
   * @return an immutable map of the nodes state
   */
  public Map<String, Object> getState();

  /**
   * @return the name of the node
   */
  public String getName();

  /**
   * @return true if this node has been elected as coordinator
   */
  public boolean isCoordinator();

  /**
   * Returns the Zone of this node - typically the DMZ zone or the subnet on which the node is on
   */
  public Object getZone();
}
