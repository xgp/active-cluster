package com.github.xgp.cluster;

/**
 * Creates {@link Cluster} members. Members are created unstarted so listeners can be added before
 * {@link Cluster#start()}. Node names must be unique within a cluster and differ from the cluster
 * name.
 *
 * @version $Revision: 1.3 $
 */
public interface ClusterFactory {

  /**
   * Creates a member named {@code localName} of the cluster named {@code destinationName}.
   *
   * @param localName the node name, unique within the cluster
   * @param destinationName the cluster name; members with the same cluster name form one cluster
   * @return the unstarted cluster member
   * @throws ClusterException if a name is not valid
   */
  public Cluster createCluster(String localName, String destinationName) throws ClusterException;

  /**
   * Creates a member named {@code localName} of the cluster named {@code destinationName}.
   *
   * @param localName the node name, unique within the cluster
   * @param destinationName the cluster name
   * @param marshaller the destination marshaller, which must suit this factory's implementation
   * @return the unstarted cluster member
   * @throws ClusterException if a name or the marshaller is not valid
   */
  public Cluster createCluster(
      String localName, String destinationName, DestinationMarshaller marshaller)
      throws ClusterException;

  /**
   * Creates a member of the cluster named {@code destinationName} with a generated, unique node
   * name.
   *
   * @param destinationName the cluster name
   * @return the unstarted cluster member
   * @throws ClusterException if the name is not valid
   */
  public Cluster createCluster(String destinationName) throws ClusterException;

  /**
   * Creates a member named {@code localName} of the cluster identified by {@code destination}.
   *
   * @param localName the node name, unique within the cluster
   * @param destination the cluster destination
   * @return the unstarted cluster member
   * @throws ClusterException if the name or destination is not valid
   */
  public Cluster createCluster(String localName, Destination destination) throws ClusterException;

  /**
   * Creates a member named {@code localName} of the cluster identified by {@code destination}.
   *
   * @param localName the node name, unique within the cluster
   * @param destination the cluster destination
   * @param marshaller the destination marshaller, which must suit this factory's implementation
   * @return the unstarted cluster member
   * @throws ClusterException if the name, destination or marshaller is not valid
   */
  public Cluster createCluster(
      String localName, Destination destination, DestinationMarshaller marshaller)
      throws ClusterException;

  /**
   * Creates a member of the cluster identified by {@code destination} with a generated, unique node
   * name.
   *
   * @param destination the cluster destination
   * @return the unstarted cluster member
   * @throws ClusterException if the destination is not valid
   */
  public Cluster createCluster(Destination destination) throws ClusterException;
}
