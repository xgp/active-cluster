# ActiveCluster

ActiveCluster is a framework for writing cluster-based applications, monitoring the available nodes in a cluster and communicating with them. ActiveCluster consists of an API together with a default implementation.

## Features

The current features include

- viewing the current nodes in the cluster and their current state
- listening to nodes arrive, update their state, shut down or fail
- communicate with the cluster as a whole or with any specific node in the cluster
- elect a leader of the cluster to make decisions on behalf of the cluster or to run singleton jobs

## Implementation

It should be easy to implement the ActiveCluster API on top of any networking library such as raw sockets, JMS, JGroups or Jabber. The default implementation uses JGroups.

### Communication

Clusters communicate across a common destination but every member (node) of a cluster has a unique name (the generation of unique names is left to the developer), and a unique destination (which uses the unique name).

### Groups

Contains Group Organsisation models and policies for arranging Node instances into groups, such as buddy-groups (failover nodes) or master/slave groups for High Availability (HA) protocols. 


# Use cases

ActiveCluster can be used to build a variety of different network protocols like...

- buddy-groups for session replication
- master / slave protocols for High Availability (HA) protocols with hot-standby
- controller-voting protocols to elect a single node in a cluster to be the controller along with defining who will be the next node if the controller falls over as well as detecting the loss of the controller
distributed lock manager




