package com.github.xgp.cluster.jgroups;

import com.github.xgp.cluster.ClusterException;
import com.github.xgp.cluster.LocalNode;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jgroups.Address;

/**
 * The member running in this process. {@link #setState} replaces the state and publishes it to the
 * other members asynchronously; state set before {@link JGroupsCluster#start()} is published when
 * the cluster starts.
 */
public class JGroupsLocalNode extends JGroupsNode implements LocalNode {

  private static final long serialVersionUID = -7213743750639969514L;

  private static final Logger LOG = Logger.getLogger(JGroupsLocalNode.class.getName());

  private transient JGroupsCluster cluster;

  /** No-arg constructor required for serialization */
  public JGroupsLocalNode() {
    super();
  }

  public JGroupsLocalNode(String name) {
    super(name);
  }

  public JGroupsLocalNode(String name, Address address) {
    super(name, address);
  }

  void setCluster(JGroupsCluster cluster) {
    this.cluster = cluster;
  }

  /**
   * Replaces this member's state (the map is copied) and publishes it to the cluster.
   *
   * @throws ClusterException if this node does not belong to a cluster, or publishing fails
   */
  @Override
  public void setState(Map<String, Object> state) throws ClusterException {
    if (cluster == null) {
      throw new ClusterException("Local node '" + getName() + "' is not attached to a cluster");
    }

    synchronized (this) {
      super.setState(state);
      setVersion(getVersion() + 1);
    }

    try {
      cluster.localStateChanged();
    } catch (Exception e) {
      throw new ClusterException(
          "Failed to publish state of '" + getName() + "': " + e.getMessage(), e);
    }
  }

  /**
   * Sets this member's zone (for example a DMZ or subnet, used by {@link
   * com.github.xgp.cluster.group.MasterZoneFilter}) and publishes it like a state change.
   */
  @Override
  public void setZone(Object zone) {
    synchronized (this) {
      super.setZone(zone);
      setVersion(getVersion() + 1);
    }
    if (cluster != null) {
      try {
        cluster.localStateChanged();
      } catch (Exception e) {
        LOG.log(Level.WARNING, "Failed to publish zone of '" + getName() + "'", e);
      }
    }
  }
}
