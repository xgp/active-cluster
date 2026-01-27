package com.github.xgp.cluster;

/**
 * A Factory of Cluster instances
 *
 * @version $Revision: 1.3 $
 */
public interface ClusterFactory {

  /**
   * Creates a new cluster connection using the given local name and destination name
   *
   * @param localName
   * @param destinationName
   * @return Cluster
   * @throws ClusterException
   */
  public Cluster createCluster(String localName, String destinationName) throws ClusterException;

  /**
   * Creates a new cluster connection using the given local name and destination name
   *
   * @param localName
   * @param destinationName
   * @param marshaller
   * @return Cluster
   * @throws ClusterException
   */
  public Cluster createCluster(
      String localName, String destinationName, DestinationMarshaller marshaller)
      throws ClusterException;

  /**
   * Creates a new cluster connection - generating the localName automatically
   *
   * @param destinationName
   * @return the Cluster
   * @throws ClusterException
   */
  public Cluster createCluster(String destinationName) throws ClusterException;

  /**
   * Creates a new cluster connection using the given local name and destination name
   *
   * @param localName
   * @param destination
   * @return Cluster
   * @throws ClusterException
   */
  public Cluster createCluster(String localName, Destination destination) throws ClusterException;

  /**
   * Creates a new cluster connection using the given local name and destination name
   *
   * @param localName
   * @param destination
   * @param marshaller
   * @return Cluster
   * @throws ClusterException
   */
  public Cluster createCluster(
      String localName, Destination destination, DestinationMarshaller marshaller)
      throws ClusterException;

  /**
   * Creates a new cluster connection - generating the localName automatically
   *
   * @param destination
   * @return the Cluster
   * @throws ClusterException
   */
  public Cluster createCluster(Destination destination) throws ClusterException;
}
