/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.xgp.cluster;

import com.github.xgp.cluster.election.ElectionStrategy;
import java.util.Map;

/**
 * Represents a logical connection to a cluster. From this object you can obtain the destination to
 * send messages to, view the members of the cluster, watch cluster events (nodes joining, leaving,
 * updating their state) as well as viewing each members state.
 *
 * <p>You may also update the local node's state.
 *
 * @version $Revision: 1.5 $
 */
public interface Cluster extends Service {

  /**
   * Returns the destination used to send a message to all members of the cluster
   *
   * @return the destination to send messages to all members of the cluster
   */
  public Destination getDestination();

  /**
   * A snapshot of the nodes in the cluster indexed by the Destination
   *
   * @return a Map containing all the nodes in the cluster, where key=node destination,value=node
   */
  public Map getNodes();

  /**
   * Adds a new listener to cluster events
   *
   * @param listener
   */
  public void addClusterListener(ClusterListener listener);

  /**
   * Removes a listener to cluster events
   *
   * @param listener
   */
  public void removeClusterListener(ClusterListener listener);

  /**
   * The local Node which allows you to mutate the state or subscribe to the nodes temporary queue
   * for inbound messages direct to the Node
   *
   * @return the Node representing this peer in the cluster
   */
  public LocalNode getLocalNode();

  /**
   * Allows overriding of the default election strategy with a custom implementation.
   *
   * @param strategy
   */
  public void setElectionStrategy(ElectionStrategy strategy);

  // Messaging helper methods
  // -------------------------------------------------------------------------

  /**
   * Sends a message to a destination, which could be to the entire group or could be a single
   * Node's destination
   *
   * @param destination is either the group topic or a node's destination
   * @param message the message to be sent
   * @throws ClusterException
   */
  public void send(Destination destination, Message message) throws ClusterException;

  /**
   * Creates a consumer of all the messags sent to the given destination, including messages sent
   * via the send() messages
   *
   * @param destination
   * @return a newly created message consumer
   * @throws ClusterException
   */
  public MessageConsumer createConsumer(Destination destination) throws ClusterException;

  /**
   * Creates a consumer of all message sent to the given destination, including messages sent via
   * the send() message with an optional SQL 92 based selector to filter messages
   *
   * @param destination
   * @param selector
   * @return a newly created message consumer
   * @throws ClusterException
   */
  public MessageConsumer createConsumer(Destination destination, String selector)
      throws ClusterException;

  /**
   * Creates a consumer of all message sent to the given destination, including messages sent via
   * the send() message with an optional SQL 92 based selector to filter messages along with
   * optionally ignoring local traffic - messages sent via the send() method on this object.
   *
   * @param destination the destination to consume from
   * @param selector an optional SQL 92 filter of messages which could be null
   * @param noLocal which if true messages sent via send() on this object will not be delivered to
   *     the consumer
   * @return a newly created message consumer
   * @throws ClusterException
   */
  public MessageConsumer createConsumer(Destination destination, String selector, boolean noLocal)
      throws ClusterException;
}
