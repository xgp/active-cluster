package com.github.xgp.cluster;

import java.util.Map;

/**
 * The member running in this process.
 *
 * @version $Revision: 1.2 $
 */
public interface LocalNode extends Node {

  /**
   * Replaces this member's state and replicates it asynchronously to the other members, who see an
   * {@code UPDATE_NODE} event. The map is copied; keys and values must be serializable.
   *
   * @param state the new state, which replaces (not merges with) the previous one
   * @throws ClusterException if the state cannot be published
   */
  public void setState(Map<String, Object> state) throws ClusterException;
}
