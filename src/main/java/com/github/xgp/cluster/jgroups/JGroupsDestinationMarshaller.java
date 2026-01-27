package com.github.xgp.cluster.jgroups;

import com.github.xgp.cluster.ClusterException;
import com.github.xgp.cluster.Destination;
import com.github.xgp.cluster.DestinationMarshaller;

/**
 * JGroups implementation of the DestinationMarshaller interface.
 *
 * @version $Revision: 1.0 $
 */
public class JGroupsDestinationMarshaller implements DestinationMarshaller {

  /**
   * Builds a destination from a destinationName
   *
   * @param destinationName
   * @return the destination to send messages to all members of the cluster
   */
  @Override
  public Destination getDestination(String destinationName) throws ClusterException {
    if (destinationName == null || destinationName.isEmpty()) {
      throw new ClusterException();
    }
    return new JGroupsDestination(destinationName);
  }

  /**
   * Gets a destination's physical name
   *
   * @param destination
   * @return the destination's physical name
   */
  @Override
  public String getDestinationName(Destination destination) {
    if (destination instanceof JGroupsDestination) {
      return ((JGroupsDestination) destination).getName();
    }
    return null;
  }
}
