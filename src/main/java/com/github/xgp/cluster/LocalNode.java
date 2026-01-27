package com.github.xgp.cluster;

import java.util.Map;

/**
 * Represents the local (in process) node
 *
 * @version $Revision: 1.2 $
 */
public interface LocalNode extends Node {

  /**
   * Allows the local state to be modified, which will be replicated asynchronously around the
   * cluster
   *
   * @param state
   * @throws ClusterException
   */
  public void setState(Map<String, Object> state) throws ClusterException;
}
