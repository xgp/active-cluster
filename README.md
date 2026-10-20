# ActiveCluster

ActiveCluster is a framework for writing cluster-based applications, monitoring the available nodes in a cluster and communicating with them. ActiveCluster consists of an API together with a default implementation.

## History

This is based on a long dead project of the same name by Codehaus. It was somewhat JMS-specific, but I liked the API, and had a working implementation ~20 years ago. I'm trying to bring this back from the bit rot, having not done anything with JGroups for that long. I'm also using it as a test to see if the language models (e.g. Claude Code) are any good at writing the implementation bits for a concise interface.

## Features

The current features include:

- Viewing the current nodes in the cluster and their current state
- Listening to nodes arrive, update their state, shut down or fail
- Communicating with the cluster as a whole or with any specific node in the cluster
- Electing a leader of the cluster to make decisions on behalf of the cluster or to run singleton jobs

## Details

It should be easy to implement the ActiveCluster API on top of any networking library such as raw sockets, JMS, JGroups or Jabber. The default implementation uses JGroups.

### Communication

Clusters communicate across a common destination but every member (node) of a cluster has a unique name (the generation of unique names is left to the developer), and a unique destination (which uses the unique name).

### Groups

Contains Group Organization models and policies for arranging Node instances into groups, such as buddy-groups (failover nodes) or master/slave groups for High Availability (HA) protocols.

### JGroups Implementation

The JGroups implementation creates a named cluster by accepting a standard JGroups XML configuration file that indicates a mechanism for node discovery (e.g. TCPPING, JDBC_PING) and communication (e.g. TCP or UDP).

To use the JGroups implementation:

```java
// Create a JGroups cluster factory
JGroupsClusterFactory factory = new JGroupsClusterFactory();

// Or specify a custom JGroups configuration file
// JGroupsClusterFactory factory = new JGroupsClusterFactory("custom-jgroups.xml");

// Create the cluster with a node name and cluster name
Cluster cluster = factory.createCluster("node1", "my-cluster");

// Start the cluster
cluster.start();

// Get the local node and set its state
Map<String, Object> state = new HashMap<>();
state.put("status", "ONLINE");
state.put("startTime", System.currentTimeMillis());
cluster.getLocalNode().setState(state);

// Send a message to the entire cluster
Message message = new JGroupsMessage("Hello Cluster!");
cluster.send(cluster.getDestination(), message);

// Create a consumer to receive messages from the cluster
MessageConsumer consumer = cluster.createConsumer(cluster.getDestination());
consumer.setMessageListener(message -> {
    System.out.println("Received: " + message.getBody(String.class));
});

// Add a listener for cluster events
cluster.addClusterListener(new ClusterListener() {
    public void onNodeAdd(ClusterEvent event) {
        System.out.println("Node added: " + event.getNode().getName());
    }
    
    // Implement other listener methods...
});

// When finished, stop the cluster
cluster.stop();
```

See the `JGroupsClusterExample` class for a complete example.

## Use Cases

ActiveCluster can be used to build a variety of different network protocols like:

- Buddy-groups for session replication
- Master/slave protocols for High Availability (HA) protocols with hot-standby
- Controller-voting protocols to elect a single node in a cluster to be the controller
- Distributed lock manager
- Work distribution across a cluster
- Cluster-wide configuration and state management

## Building and Running

To build the project:

```bash
mvn clean install
```

To run the tests:

```bash
# Unit and in-JVM cluster tests (JGroups SHARED_LOOPBACK plus one real TCP scenario on 127.0.0.1)
mvn test

# Additionally run the cross-JVM test, which launches the example app in separate processes
# over TCP on 127.0.0.1 ports 17800-17820
mvn verify -Pmultiprocess
```

The formatter (`mvn fmt:format`) needs JDK 21; its bundled google-java-format does not run on
JDK 25.

To run the example application:

```bash
# Basic execution with default configuration 
mvn exec:java -Dexec.args="node1"

# With specific node name and configuration file
mvn exec:java -Dexec.args="node1 udp.xml"
```

The JGroups implementation includes several configuration options:
- `minimal.xml` - Absolute minimal configuration with no custom parameters (default)
- `simple-udp.xml` - Basic UDP configuration with some optimized settings
- `udp.xml` - Enhanced UDP multicast for discovery (works well in local development)
- `tcp.xml` - Uses TCP with static configuration (better for production)

For maximum compatibility, use the minimal.xml configuration:

```bash
mvn exec:java -Dexec.args="node1 minimal.xml"
```


