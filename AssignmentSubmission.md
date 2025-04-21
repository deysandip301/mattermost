# Enhancing Mattermost Open Source: High Availability and Horizontal Scalability

## Project Overview

This project enhances the open-source version of Mattermost to support high availability (HA) and horizontal scalability by implementing a Redis-based cluster solution. The implementation enables multiple Mattermost servers to work together seamlessly, providing fault tolerance, load distribution, and consistent real-time communication.

## Approach

### Problem Analysis

The original open-source version of Mattermost faced several critical limitations:

1. **Single Point of Failure**: If the server went down, the entire service became unavailable
2. **Limited Load Distribution**: The system couldn't scale horizontally to handle increased user loads
3. **Poor Fault Tolerance**: Component failures weren't gracefully handled
4. **Lack of Real-time Synchronization**: Events like user presence and typing indicators weren't synchronized across servers

### Solution Design Principles

I approached the solution with the following key principles:

1. **Minimal Changes to Core Code**: Make changes that integrate well with existing architecture
2. **Optimized for Performance**: Ensure high performance even with distributed communication
3. **Reliability First**: Implement reliable message delivery for critical events
4. **Ease of Configuration**: Make the cluster setup straightforward for administrators

### Implementation Strategy

The solution revolves around a Redis-based cluster mechanism that provides:

1. **Inter-node Communication**: Real-time message propagation between nodes
2. **Distributed Leadership**: Leader election for cluster-wide coordination tasks
3. **Event Broadcasting**: WebSocket events broadcasted to all connected clients regardless of which node they're connected to
4. **Health Monitoring**: Continuous health checks and synchronization verification

## System Architecture

### High-Level Architecture

The enhanced architecture consists of:

1. **Multiple Mattermost Application Nodes**: Horizontally scalable application nodes behind a load balancer
2. **Redis Cluster**: For inter-node communication, caching, and pub/sub messaging
3. **Shared PostgreSQL Database**: For consistent data storage across nodes

```ascii
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│                           Load Balancer                                 │
│                                                                         │
└───────────────────────────────┬───────────────────────────────┬─────────┘
                                │                               │
                                │                               │
                                ▼                               ▼
┌───────────────────────────────┴───────────────────────────────┴─────────┐
│                                                                         │
│            ┌─────────────────────┐             ┌─────────────────────┐ │
│            │                     │             │                     │ │
│            │  Mattermost Node 1  │             │  Mattermost Node 2  │ │
│            │     (Port 8065)     │             │     (Port 8066)     │ │
│            │                     │             │                     │ │
│            └──────────┬──────────┘             └──────────┬──────────┘ │
│                       │                                    │            │
│                       │                                    │            │
│                       └─────────────────┬─────────────────┘            │
│                                         │                               │
│                                         │                               │
│                                         ▼                               │
│            ┌────────────────────────────────────────────────────────────┐│
│            │                                                            ││
│            │                      Redis Server                          ││
│            │                       (Port 6379)                          ││
│            │                                                            ││
│            └────────────────────────────────────────────────────────────┘│
│                                                                         │
└─────────────────────────────────────┬───────────────────────────────────┘
                                      │
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                                                                 │   │
│  │                    PostgreSQL Database                          │   │
│  │                      (Port 5432)                                │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Development and Testing Setup

For development and testing purposes, I implemented a two-node setup on a single machine using different ports. This approach allows developers to:

1. Test the cluster functionality on a single development machine
2. Verify real-time communication between nodes
3. Simulate node failures and recovery

## Development and Testing Environment

### Multi-Node Cluster Setup for Development

For development and testing purposes, I've implemented a simple yet effective approach to run a multi-node Mattermost cluster on a single machine. This setup allows for testing the horizontal scalability and high availability features without requiring multiple physical servers.

#### Setup Components

1. **Two Mattermost Nodes**: Independent server processes running on different ports (8065 and 8066)
2. **Shared Redis Server**: For inter-node communication and synchronization
3. **Shared PostgreSQL Database**: For consistent data storage across nodes

#### Running the Cluster

I've created simple scripts to facilitate running and managing the cluster:

1. **Starting Node 1 (Default)**:
   ```bash
   # Node 1 uses default configuration
   make run-server
   ```

2. **Starting Node 2**:
   ```bash
   # Run the second node on port 8066
   ./server/run_node2.sh
   ```

3. **Checking Cluster Status**:
   ```bash
   # Verify both nodes are running properly
   ./server/check-cluster.sh
   ```

#### Technical Details

Each node is configured with environment variables to ensure proper separation:

```bash
# Node 2 Configuration
export GO_BUILD_TAGS="mm_dev_cluster"  # Enable cluster-specific code
export MM_FILESETTINGS_DIRECTORY="/tmp/mmdata2"  # Separate storage
export MM_SERVICESETTINGS_LISTENADDRESS=":8066"  # Different port
export MM_SQLSETTINGS_DATASOURCE="postgres://mmuser:mostest@localhost/mattermost_test?sslmode=disable"
export MM_CACHESETTINGS_REDISADDRESS="localhost:6379"
export MM_CLUSTERSETTINGS_ENABLE="true"
```

This approach provides several advantages:
- Simple to set up and use
- Uses Mattermost's built-in configuration system
- Both nodes share the same database but maintain separate file storage
- Real-time synchronization via Redis

#### Testing the Cluster

With both nodes running, you can verify proper cluster functionality by:

1. Opening separate browser windows for each node:
   - Node 1: http://localhost:8065
   - Node 2: http://localhost:8066

2. Logging in with the same user on both nodes

3. Sending messages on one node and verifying they appear on the other

4. Testing real-time features like presence updates and typing indicators across nodes

This development setup demonstrates the same principles that would be used in a production deployment but in a controlled, single-machine environment that's ideal for development and testing.

### Setup Scripts

1. **setup-cluster.sh**: Prepares the environment for clustering by:
   - Ensuring Redis is installed and running
   - Configuring the main config file for Redis and clustering
   - Creating a second config file for the second node with a different port

2. **run-node1.sh**: Starts the first Mattermost node on port 8065 using the default make command:
   ```bash
   # Starting the first node
   ./scripts/run-node1.sh
   ```

3. **run-node2.sh**: Starts the second Mattermost node on port 8066 with an alternate config:
   ```bash
   # Starting the second node
   ./scripts/run-node2.sh
   ```

4. **test-cluster.sh**: Verifies the cluster is working correctly by:
   - Checking if both nodes are responding
   - Verifying Redis connectivity
   - Retrieving and comparing cluster information from both nodes

### Running the Cluster Locally

To run a two-node Mattermost cluster on your development machine:

1. Set up the environment:
   ```bash
   ./scripts/setup-cluster.sh
   ```

2. Open two terminal windows and start each node:
   ```bash
   # Terminal 1
   ./scripts/run-node1.sh

   # Terminal 2
   ./scripts/run-node2.sh
   ```

3. Access the nodes in your browser:
   - Node 1: http://localhost:8065
   - Node 2: http://localhost:8066

4. Test cluster functionality:
   ```bash
   ./scripts/test-cluster.sh
   ```

This setup allows developers to:
1. Test cluster communication on a single machine
2. Develop and debug cluster features without requiring multiple servers
3. Simulate node failures by stopping one of the node processes

### Key Components

1. **Redis-Based Cluster Communication**:
   - Real-time event propagation between nodes
   - Cluster-wide caching to improve performance
   - Leader election for coordinated tasks
   - Reliable message delivery for critical operations

2. **WebSocket Event Distribution**:
   - Seamless WebSocket message distribution across nodes
   - Synchronized user presence information
   - Real-time typing indicators and notifications

3. **Centralized Configuration Management**:
   - Configuration synced across all nodes
   - Dynamic updates without restarts

## Key Code Changes

### 1. Redis Cluster Implementation
Implemented a comprehensive Redis-based cluster service that handles:
- Inter-node communication
- Leader election
- Reliable message delivery
- WebSocket event broadcasting

```go
// Implementation of Redis-based cluster service
func InitRedisCluster(ps *PlatformService) einterfaces.ClusterInterface {
    // Check if Redis cache is enabled
    if !*ps.Config().CacheSettings.EnableRedisCache {
        ps.logger.Error("Clustering requires Redis cache to be enabled")
        return nil
    }
    
    // Create broadcast hooks for WebSocket events
    hooks := make(map[string]BroadcastHook)
    
    // Initialize Redis cluster with hooks
    rc := newCommunityCluster(ps, hooks)
    
    // Start the cluster service
    if err := rc.Start(); err != nil {
        ps.logger.Error("Failed to start Redis cluster", mlog.Err(err))
        return nil
    }
    
    return rc
}
```

### 2. WebSocket Event Distribution
Enhanced WebSocket event handling to ensure real-time communication across nodes:

```go
// BroadcastWebSocketEvent broadcasts WebSocket events to all nodes
func (rc *communityCluster) BroadcastWebSocketEvent(event *model.WebSocketEvent) {
    // Skip if the event is already from another cluster node
    if event.IsFromCluster() {
        return
    }

    // Mark the event as coming from cluster
    event = event.SetFromCluster(true)

    // Convert WebSocketEvent to JSON bytes
    data, err := event.ToJSON()
    if err != nil {
        rc.logger.Error("Failed to marshal WebSocket event", mlog.Err(err))
        return
    }

    // Create cluster message
    msg := &model.ClusterMessage{
        Event:    model.ClusterEventPublish,
        Data:     data,
        SendType: model.ClusterSendReliable,
    }

    // Send to other nodes via Redis
    rc.SendClusterMessage(msg)
}
```

### 3. WebSocket Model Extensions
Added cluster awareness to WebSocket events:

```go
// WebSocketEvent extended with cluster awareness
func (ev *WebSocketEvent) IsFromCluster() bool {
    return ev.fromCluster
}

func (ev *WebSocketEvent) SetFromCluster(fromCluster bool) *WebSocketEvent {
    evCopy := ev.Copy()
    evCopy.fromCluster = fromCluster
    return evCopy
}
```

### 4. Reliable Message Delivery
Implemented a reliable message delivery system to ensure critical messages reach all nodes:

```go
// Send cluster message with reliability guarantees
func (rc *communityCluster) SendClusterMessage(msg *model.ClusterMessage) {
    // Handle reliable message delivery
    if rc.reliableEnabled && msg.SendType == model.ClusterSendReliable {
        key := fmt.Sprintf("%s:%s:%s", redisReliablePrefix, string(msg.Event), msg.Props["msg_id"])

        // Store message in Redis for reliable delivery
        rc.rdb.Set(ctx, key, string(data), reliableMessageTTL)
        
        // Add to list of reliable messages to process
        listKey := fmt.Sprintf("%s:list", redisReliablePrefix)
        rc.rdb.RPush(ctx, listKey, key)
        
        // Ensure list has TTL
        rc.rdb.Expire(ctx, listKey, reliableMessageTTL)
    }

    // Publish message to all nodes
    rc.rdb.Publish(ctx, redisPubSubChannel, string(data))
}
```

## Challenges Faced and Solutions

### 1. Message Ordering and Consistency
**Challenge**: Ensuring messages appear in the same order across all nodes  
**Solution**: Implemented a centralized timestamp service with Redis for consistent ordering

### 2. Real-time Presence Information
**Challenge**: Synchronizing user presence data across multiple servers  
**Solution**: Created a reliable presence propagation system using Redis pub/sub with heartbeats

### 3. WebSocket Connection Distribution
**Challenge**: Handling WebSocket connections across multiple servers  
**Solution**: Developed a sticky session mechanism for the load balancer while ensuring fallback capability

### 4. Redis Connection Management
**Challenge**: Handling Redis connection failures gracefully  
**Solution**: Implemented connection pooling, retry logic, and circuit breaker patterns

### 5. Configuration Synchronization
**Challenge**: Ensuring consistent configuration across all nodes  
**Solution**: Developed a Redis-based configuration synchronization system

## Testing and Validation

### Test Scenarios
1. **Multi-Server Communication Test**: Verified seamless communication between users connected to different servers
2. **Node Failure Recovery Test**: Tested graceful handling of node failures
3. **Load Distribution Test**: Verified proper distribution of user connections across nodes
4. **Real-time Feature Test**: Validated that typing indicators, presence updates, and notifications work across nodes
5. **Performance Test**: Benchmarked system performance under varying loads

### Results
- Successfully achieved seamless communication between users on different nodes
- System maintained availability during simulated node failures
- Real-time features (typing indicators, presence) worked consistently across all nodes
- System scaled horizontally to handle increasing user loads

## Future Improvements

1. **Enhanced Monitoring**: Add comprehensive monitoring and alerting specific to cluster operations
2. **Automated Scaling**: Implement auto-scaling capabilities based on load metrics
3. **Geographic Distribution**: Support for multi-region deployment for global scale
4. **Redis Cluster Support**: Extend support for Redis Cluster mode for higher availability
5. **Advanced Message Queueing**: Implement more sophisticated message prioritization and queueing strategies

## Conclusion

This project successfully transformed the open-source version of Mattermost into a horizontally scalable, high-availability system. The Redis-based cluster implementation enables seamless communication across multiple servers, consistent message handling, and reliable real-time features. These enhancements make the open-source version suitable for production environments where uptime, fault tolerance, and scalability are essential requirements.
