package com.github.xgp.cluster;

/**
 * A simple marshaller for Destinations
 *
 * @version $Revision: 1.5 $
 */
public interface DestinationMarshaller {

  /**
   * Builds a destination from a destinationName
   *
   * @param destinationName
   * @return the destination to send messages to all members of the cluster
   */
  public Destination getDestination(String destinationName) throws ClusterException;

  /**
   * Gets a destination's physical name
   *
   * @param destination
   * @return the destination's physical name
   */
  public String getDestinationName(Destination destination);
}
