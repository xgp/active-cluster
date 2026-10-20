package com.github.xgp.cluster;

/**
 * Converts between destinations and their names.
 *
 * @version $Revision: 1.5 $
 */
public interface DestinationMarshaller {

  /**
   * Builds a destination from its name.
   *
   * @param destinationName the name of a cluster or of a node
   * @return the destination with that name
   * @throws ClusterException if the name is not valid
   */
  public Destination getDestination(String destinationName) throws ClusterException;

  /**
   * Gets a destination's name.
   *
   * @param destination the destination
   * @return the destination's name, or null if the destination is not supported
   */
  public String getDestinationName(Destination destination);
}
