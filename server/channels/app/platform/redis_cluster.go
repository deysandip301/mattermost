package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/shared/mlog"
	"github.com/mattermost/mattermost/server/public/shared/request"
	"github.com/mattermost/mattermost/server/v8/einterfaces"
)

const (
	// Redis keys and channels
	redisClusterChannel        = "mattermost_cluster"
	redisClusterLeaderKey      = "mattermost_cluster_leader"
	redisClusterNodesKey       = "mattermost_cluster_nodes"
	redisClusterEventChannel   = "mattermost_cluster_events"
	redisClusterReliablePrefix = "mattermost_cluster_reliable"

	// Timing constants
	clusterLeaderTTL             = 15 * time.Second
	clusterNodeTTL               = 20 * time.Second
	clusterHeartbeatInterval     = 5 * time.Second
	clusterSyncTimeout           = 10 * time.Second
	clusterReliableMsgTTL        = 24 * time.Hour
	clusterProcessDelay          = 500 * time.Millisecond
	clusterRetryInterval         = 5 * time.Second
	clusterLeaderElectionBackoff = 1 * time.Second

	// Operation constants
	maxClusterMessageBuffer  = 1000
	maxClusterBatchSize      = 100
	clusterBatchTimeout      = 100 * time.Millisecond
	maxLeaderElectionRetries = 3
)

// RedisClusterError represents errors specific to the Redis cluster implementation
type RedisClusterError struct {
	Operation string
	Err       error
}

func (e *RedisClusterError) Error() string {
	return fmt.Sprintf("redis cluster error during %s: %v", e.Operation, e.Err)
}

func (e *RedisClusterError) Unwrap() error {
	return e.Err
}

// RedisClusterHealth provides health metrics for the Redis cluster implementation
type RedisClusterHealth struct {
	IsLeader         bool
	IsReady          bool
	MessageQueueSize int
	LastSyncTime     time.Time
	NodeCount        int
	RedisConnected   bool
	Errors           []error
}

// RedisCluster implements the ClusterInterface using Redis pub/sub
type RedisCluster struct {
	platformService   *PlatformService
	nodeID            string
	redisClient       *redis.Client
	redisPubSub       *redis.PubSub
	eventHandlers     map[model.ClusterEvent][]einterfaces.ClusterMessageHandler
	handlersMutex     sync.RWMutex
	isLeader          atomic.Bool
	isReady           atomic.Bool
	ctx               context.Context
	cancel            context.CancelFunc
	stopChan          chan struct{}
	waitGroup         sync.WaitGroup
	logger            *mlog.Logger
	messageBuffer     chan model.ClusterMessage
	lastSyncTimestamp atomic.Int64
	syncVersion       atomic.Int64
	broadcastHooks    map[string]BroadcastHook
	processedMessages sync.Map
	retryQueue        chan *model.ClusterMessage
	reliableEnabled   bool
}

// InitRedisCluster creates and initializes a new Redis-based cluster implementation
func InitRedisCluster(ps *PlatformService) einterfaces.ClusterInterface {
	// Create broadcast hooks map
	broadcastHooks := make(map[string]BroadcastHook)

	// Initialize Redis cluster with hooks
	cluster := newRedisCluster(ps, broadcastHooks)
	if cluster == nil {
		return nil
	}

	if err := cluster.Start(); err != nil {
		ps.logger.Error("Failed to start Redis cluster", mlog.Err(err))
		return nil
	}

	return cluster
}

// newRedisCluster creates a new Redis-based cluster implementation
func newRedisCluster(ps *PlatformService, hooks map[string]BroadcastHook) *RedisCluster {
	nodeID := model.NewId()
	ctx, cancel := context.WithCancel(context.Background())

	rc := &RedisCluster{
		platformService: ps,
		nodeID:          nodeID,
		eventHandlers:   make(map[model.ClusterEvent][]einterfaces.ClusterMessageHandler),
		stopChan:        make(chan struct{}),
		logger:          ps.logger.With(mlog.String("cluster_node_id", nodeID)),
		messageBuffer:   make(chan model.ClusterMessage, maxClusterMessageBuffer),
		ctx:             ctx,
		cancel:          cancel,
		broadcastHooks:  hooks,
		retryQueue:      make(chan *model.ClusterMessage, 1000),
		reliableEnabled: true,
	}

	// Initialize Redis client from config
	cacheConfig := ps.Config().CacheSettings
	rc.redisClient = redis.NewClient(&redis.Options{
		Addr:     *cacheConfig.RedisAddress,
		Password: *cacheConfig.RedisPassword,
		DB:       int(*cacheConfig.RedisDB),
	})

	// Register the platform's cluster handlers
	rc.RegisterClusterMessageHandler(model.ClusterEventPublish, ps.ClusterPublishHandler)
	rc.RegisterClusterMessageHandler(model.ClusterEventUpdateStatus, ps.ClusterUpdateStatusHandler)
	rc.RegisterClusterMessageHandler(model.ClusterEventInvalidateAllCaches, ps.ClusterInvalidateAllCachesHandler)
	rc.RegisterClusterMessageHandler(model.ClusterEventInvalidateWebConnCacheForUser, ps.clusterInvalidateWebConnSessionCacheForUserHandler)
	rc.RegisterClusterMessageHandler(model.ClusterEventBusyStateChanged, ps.clusterBusyStateChgHandler)

	return rc
}

// Start initializes and starts the Redis cluster communication
func (rc *RedisCluster) Start() error {
	// Test Redis connection with timeout
	ctx, cancel := context.WithTimeout(rc.ctx, 5*time.Second)
	defer cancel()

	if err := rc.redisClient.Ping(ctx).Err(); err != nil {
		return &RedisClusterError{
			Operation: "redis_connection",
			Err:       fmt.Errorf("failed to connect to Redis: %w", err),
		}
	}

	// Subscribe to cluster events
	rc.redisPubSub = rc.redisClient.Subscribe(rc.ctx, redisClusterChannel, redisClusterEventChannel)

	// Start cluster routines
	rc.waitGroup.Add(7)
	go rc.leaderElectionLoop()
	go rc.heartbeatLoop()
	go rc.messageListener()
	go rc.messageProcessor()
	go rc.syncLoop()
	go rc.processReliableMessages()
	go rc.processRetryQueue()

	// Mark the node as ready
	rc.isReady.Store(true)

	rc.logger.Info("Redis cluster started",
		mlog.String("node_id", rc.nodeID),
		mlog.String("redis_address", *rc.platformService.Config().CacheSettings.RedisAddress),
		mlog.Bool("reliable_enabled", rc.reliableEnabled))

	return nil
}

// Stop gracefully shuts down the Redis cluster communication
func (rc *RedisCluster) Stop() {
	rc.logger.Info("Stopping Redis cluster", mlog.String("node_id", rc.nodeID))
	rc.cancel()
	close(rc.stopChan)
	if rc.redisPubSub != nil {
		rc.redisPubSub.Close()
	}
	rc.waitGroup.Wait()
	rc.logger.Info("Redis cluster stopped", mlog.String("node_id", rc.nodeID))
}

// leaderElectionLoop periodically attempts to become the cluster leader
func (rc *RedisCluster) leaderElectionLoop() {
	defer rc.waitGroup.Done()
	ticker := time.NewTicker(clusterLeaderTTL / 2)
	defer ticker.Stop()

	for {
		select {
		case <-rc.ctx.Done():
			return
		case <-ticker.C:
			rc.tryBecomeLeader()
		}
	}
}

// tryBecomeLeader attempts to acquire the leader role for this node
func (rc *RedisCluster) tryBecomeLeader() error {
	ctx, cancel := context.WithTimeout(rc.ctx, 2*time.Second)
	defer cancel()

	wasLeader := rc.IsLeader()
	var lastErr error

	// Retry leader election with backoff
	for i := 0; i < maxLeaderElectionRetries; i++ {
		if i > 0 {
			time.Sleep(clusterLeaderElectionBackoff)
		}

		// Try to become leader using SET NX (only set if not exists)
		success, err := rc.redisClient.SetNX(ctx, redisClusterLeaderKey, rc.nodeID, clusterLeaderTTL).Result()
		if err != nil {
			lastErr = err
			rc.logger.Error("Failed to perform leader election",
				mlog.Err(err),
				mlog.String("node_id", rc.nodeID),
				mlog.Int("attempt", i+1))
			continue
		}

		// If not successful, verify who is the leader
		if !success {
			// Check who is the current leader
			leaderID, err := rc.redisClient.Get(ctx, redisClusterLeaderKey).Result()
			if err != nil {
				if err != redis.Nil {
					lastErr = err
					rc.logger.Error("Failed to get current leader",
						mlog.Err(err),
						mlog.String("node_id", rc.nodeID))
				}
				// Not the leader
				rc.isLeader.Store(false)
			} else {
				// Still the leader if the ID matches
				rc.isLeader.Store(leaderID == rc.nodeID)
			}
		} else {
			// Successfully became the leader
			rc.isLeader.Store(true)
		}

		isLeader := rc.isLeader.Load()

		// Set sync version for this node
		_, err = rc.redisClient.Set(ctx, fmt.Sprintf("%s:sync:%s", redisClusterNodesKey, rc.nodeID), rc.syncVersion.Load(), clusterNodeTTL).Result()
		if err != nil {
			rc.logger.Error("Failed to set sync version",
				mlog.Err(err),
				mlog.String("node_id", rc.nodeID))
		}

		// Check if leader status changed
		if wasLeader != isLeader {
			if isLeader {
				rc.logger.Info("Became cluster leader", mlog.String("node_id", rc.nodeID))
				// Trigger immediate sync when becoming leader
				go rc.verifyClusterSync()
			} else {
				rc.logger.Info("Lost cluster leadership", mlog.String("node_id", rc.nodeID))
			}
			// Notify platform service
			rc.platformService.InvokeClusterLeaderChangedListeners()
		}

		// If leader, refresh TTL to maintain leadership
		if isLeader {
			_, err := rc.redisClient.Expire(ctx, redisClusterLeaderKey, clusterLeaderTTL).Result()
			if err != nil {
				rc.logger.Error("Failed to refresh leader TTL",
					mlog.Err(err),
					mlog.String("node_id", rc.nodeID))
			}
			return nil
		}
	}

	return &RedisClusterError{
		Operation: "leader_election",
		Err:       fmt.Errorf("failed after %d attempts: %w", maxLeaderElectionRetries, lastErr),
	}
}

// heartbeatLoop periodically sends heartbeat to show node is alive
func (rc *RedisCluster) heartbeatLoop() {
	defer rc.waitGroup.Done()
	ticker := time.NewTicker(clusterHeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rc.ctx.Done():
			return
		case <-ticker.C:
			rc.sendHeartbeat()
		}
	}
}

// sendHeartbeat updates this node's status in Redis
func (rc *RedisCluster) sendHeartbeat() {
	ctx, cancel := context.WithTimeout(rc.ctx, 2*time.Second)
	defer cancel()

	nodeInfo := &model.ClusterInfo{
		Id:      rc.nodeID,
		Version: model.CurrentVersion,
	}

	data, err := json.Marshal(nodeInfo)
	if err != nil {
		rc.logger.Error("Failed to marshal node info",
			mlog.Err(err),
			mlog.String("node_id", rc.nodeID))
		return
	}

	// Use pipeline for multiple operations
	pipe := rc.redisClient.Pipeline()

	// Set node info with TTL
	nodeKey := fmt.Sprintf("%s:%s", redisClusterNodesKey, rc.nodeID)
	pipe.Set(ctx, nodeKey, string(data), clusterNodeTTL)

	// Set sync version with TTL
	syncKey := fmt.Sprintf("%s:sync:%s", redisClusterNodesKey, rc.nodeID)
	pipe.Set(ctx, syncKey, rc.syncVersion.Load(), clusterNodeTTL)

	_, err = pipe.Exec(ctx)
	if err != nil {
		rc.logger.Error("Failed to send heartbeat",
			mlog.Err(err),
			mlog.String("node_id", rc.nodeID))
	}
}

// messageListener listens for messages on the Redis pub/sub channels
func (rc *RedisCluster) messageListener() {
	defer rc.waitGroup.Done()
	ch := rc.redisPubSub.Channel()

	for {
		select {
		case <-rc.ctx.Done():
			return
		case msg := <-ch:
			if msg == nil {
				continue
			}

			switch msg.Channel {
			case redisClusterChannel:
				rc.handleClusterMessage(msg.Payload)
			case redisClusterEventChannel:
				rc.handleEventMessage(msg.Payload)
			}
		}
	}
}

// messageProcessor batches and processes cluster messages
func (rc *RedisCluster) messageProcessor() {
	defer rc.waitGroup.Done()

	batch := make([]model.ClusterMessage, 0, maxClusterBatchSize)
	batchTimer := time.NewTimer(clusterBatchTimeout)
	defer batchTimer.Stop()

	for {
		select {
		case <-rc.ctx.Done():
			return
		case msg := <-rc.messageBuffer:
			if !rc.isReady.Load() {
				rc.logger.Debug("Node not ready, skipping message",
					mlog.String("event", string(msg.Event)),
					mlog.String("node_id", rc.nodeID))
				continue
			}

			batch = append(batch, msg)
			if len(batch) >= maxClusterBatchSize {
				rc.processMessageBatch(batch)
				batch = batch[:0]
				batchTimer.Reset(clusterBatchTimeout)
			}
		case <-batchTimer.C:
			if len(batch) > 0 {
				rc.processMessageBatch(batch)
				batch = batch[:0]
			}
			batchTimer.Reset(clusterBatchTimeout)
		}
	}
}

// processMessageBatch processes a batch of cluster messages
func (rc *RedisCluster) processMessageBatch(messages []model.ClusterMessage) {
	// Use pipeline for batch processing
	pipe := rc.redisClient.Pipeline()
	ctx, cancel := context.WithTimeout(rc.ctx, 5*time.Second)
	defer cancel()

	for _, msg := range messages {
		// Skip if this message came from this node
		if msg.Props != nil && msg.Props["source_node_id"] == rc.nodeID {
			continue
		}

		// Check for duplicates
		msgID := msg.Props["msg_id"]
		if msgID != "" {
			if _, exists := rc.processedMessages.LoadOrStore(msgID, true); exists {
				continue
			}
			// Cleanup after processing to avoid memory leak
			defer func(id string) {
				time.AfterFunc(5*time.Minute, func() {
					rc.processedMessages.Delete(id)
				})
			}(msgID)
		}

		rc.logger.Debug("Processing message",
			mlog.String("event", string(msg.Event)),
			mlog.String("node_id", rc.nodeID),
			mlog.String("msg_id", msgID))

		// Handle WebSocket events
		if msg.Event == model.ClusterEventPublish {
			wsMsg, err := model.WebSocketEventFromJSON(bytes.NewReader(msg.Data))
			if err != nil {
				rc.logger.Error("Failed to deserialize WebSocket event",
					mlog.Err(err),
					mlog.String("node_id", rc.nodeID))
				continue
			}
			// Set from cluster to avoid re-sending
			wsMsg = wsMsg.SetFromCluster(true)
			rc.platformService.PublishSkipClusterSend(wsMsg)
			continue
		}

		// Get handlers for this event
		rc.handlersMutex.RLock()
		handlers, ok := rc.eventHandlers[msg.Event]
		rc.handlersMutex.RUnlock()

		if !ok {
			continue
		}

		// Call all registered handlers
		for _, handler := range handlers {
			handler(&msg)
		}
	}

	// Execute pipeline
	if _, err := pipe.Exec(ctx); err != nil {
		rc.logger.Error("Failed to execute message batch",
			mlog.Err(err),
			mlog.Int("batch_size", len(messages)))
	}
}

// handleClusterMessage processes a message from the cluster channel
func (rc *RedisCluster) handleClusterMessage(payload string) {
	var msg model.ClusterMessage
	if err := json.Unmarshal([]byte(payload), &msg); err != nil {
		rc.logger.Error("Failed to unmarshal cluster message",
			mlog.Err(err),
			mlog.String("node_id", rc.nodeID))
		return
	}

	// Ensure Props is initialized
	if msg.Props == nil {
		msg.Props = make(map[string]string)
	}

	rc.logger.Debug("Received cluster message",
		mlog.String("event", string(msg.Event)),
		mlog.String("node_id", rc.nodeID),
		mlog.Bool("has_handler", rc.hasHandler(msg.Event)))

	// Add to message buffer
	select {
	case rc.messageBuffer <- msg:
		// Message buffered successfully
	default:
		rc.logger.Warn("Message buffer full, dropping message",
			mlog.String("event", string(msg.Event)),
			mlog.String("node_id", rc.nodeID))
	}
}

// handleEventMessage processes a message from the event channel
func (rc *RedisCluster) handleEventMessage(payload string) {
	var ev model.PluginClusterEvent
	if err := json.Unmarshal([]byte(payload), &ev); err != nil {
		rc.logger.Error("Failed to unmarshal plugin event", mlog.Err(err))
		return
	}

	rc.handlersMutex.RLock()
	handlers, ok := rc.eventHandlers[model.ClusterEventPluginEvent]
	rc.handlersMutex.RUnlock()

	if !ok {
		return
	}

	msg := &model.ClusterMessage{
		Event: model.ClusterEventPluginEvent,
		Props: map[string]string{
			"EventId":        ev.Id,
			"source_node_id": rc.nodeID,
			"msg_id":         model.NewId(),
		},
		Data: ev.Data,
	}

	for _, handler := range handlers {
		handler(msg)
	}
}

// hasHandler checks if handlers are registered for an event
func (rc *RedisCluster) hasHandler(event model.ClusterEvent) bool {
	rc.handlersMutex.RLock()
	defer rc.handlersMutex.RUnlock()
	_, ok := rc.eventHandlers[event]
	return ok
}

// syncLoop periodically verifies cluster synchronization
func (rc *RedisCluster) syncLoop() {
	defer rc.waitGroup.Done()
	ticker := time.NewTicker(clusterHeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rc.ctx.Done():
			return
		case <-ticker.C:
			rc.verifyClusterSync()
		}
	}
}

// verifyClusterSync checks if all nodes are in sync
func (rc *RedisCluster) verifyClusterSync() error {
	// Get all cluster nodes
	nodes := rc.GetClusterInfos()
	if len(nodes) == 0 {
		rc.logger.Warn("No cluster nodes found during sync verification",
			mlog.String("node_id", rc.nodeID))
		return fmt.Errorf("no cluster nodes found")
	}

	// Check version consistency
	for _, node := range nodes {
		if node.Version != model.CurrentVersion {
			rc.logger.Error("Version mismatch detected",
				mlog.String("node_id", rc.nodeID),
				mlog.String("remote_node_id", node.Id),
				mlog.String("local_version", model.CurrentVersion),
				mlog.String("remote_version", node.Version))
			return fmt.Errorf("version mismatch with node %s", node.Id)
		}
	}

	// Update sync timestamp
	rc.lastSyncTimestamp.Store(time.Now().UnixNano())
	return nil
}

// processReliableMessages processes messages that were sent reliably
func (rc *RedisCluster) processReliableMessages() {
	defer rc.waitGroup.Done()
	ticker := time.NewTicker(clusterProcessDelay)
	defer ticker.Stop()

	for {
		select {
		case <-rc.ctx.Done():
			return
		case <-ticker.C:
			if !rc.isReady.Load() {
				continue
			}

			ctx, cancel := context.WithTimeout(rc.ctx, 5*time.Second)

			// Get the list of reliable messages
			listKey := fmt.Sprintf("%s:list", redisClusterReliablePrefix)

			// Get up to 100 messages at a time
			keys, err := rc.redisClient.LRange(ctx, listKey, 0, 99).Result()
			if err != nil {
				rc.logger.Error("Failed to get reliable message list",
					mlog.Err(err),
					mlog.String("node_id", rc.nodeID))
				cancel()
				continue
			}

			// Process each message
			for _, key := range keys {
				// Get the message
				data, err := rc.redisClient.Get(ctx, key).Result()
				if err != nil {
					if err != redis.Nil {
						rc.logger.Error("Failed to get reliable message",
							mlog.Err(err),
							mlog.String("key", key))
					}

					// Remove from list regardless since it doesn't exist
					rc.redisClient.LRem(ctx, listKey, 1, key)
					continue
				}

				// Parse the message
				var msg model.ClusterMessage
				if err := json.Unmarshal([]byte(data), &msg); err != nil {
					rc.logger.Error("Failed to unmarshal reliable message",
						mlog.Err(err),
						mlog.String("key", key))

					// Remove invalid message
					rc.redisClient.LRem(ctx, listKey, 1, key)
					continue
				}

				// Process the message
				rc.logger.Debug("Processing reliable message",
					mlog.String("event", string(msg.Event)),
					mlog.String("msg_id", msg.Props["msg_id"]))

				// Add to message buffer
				select {
				case rc.messageBuffer <- msg:
					// Successfully buffered
				default:
					rc.logger.Warn("Message buffer full, will retry reliable message later",
						mlog.String("event", string(msg.Event)))
					// Continue without removing from the list
					continue
				}

				// Remove from list after processing
				rc.redisClient.LRem(ctx, listKey, 1, key)
			}

			cancel()
		}
	}
}

// processRetryQueue processes messages that failed to be sent
func (rc *RedisCluster) processRetryQueue() {
	defer rc.waitGroup.Done()
	ticker := time.NewTicker(clusterRetryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rc.ctx.Done():
			return
		case <-ticker.C:
			// Process retry queue if ready
			if !rc.isReady.Load() {
				continue
			}

		retryLoop:
			for {
				select {
				case msg := <-rc.retryQueue:
					rc.logger.Debug("Retrying message from queue",
						mlog.String("event", string(msg.Event)),
						mlog.String("msg_id", msg.Props["msg_id"]))

					data, err := json.Marshal(msg)
					if err != nil {
						rc.logger.Error("Failed to marshal retry message",
							mlog.Err(err),
							mlog.String("event", string(msg.Event)))
						continue
					}

					ctx, cancel := context.WithTimeout(rc.ctx, 2*time.Second)

					// Determine channel based on event type
					channel := redisClusterChannel
					if msg.Event == model.ClusterEventPluginEvent {
						channel = redisClusterEventChannel
					}

					// Publish the message
					err = rc.redisClient.Publish(ctx, channel, string(data)).Err()
					if err != nil {
						rc.logger.Error("Failed to publish retry message",
							mlog.Err(err),
							mlog.String("event", string(msg.Event)))

						// Put back in queue if it's still reliable
						if msg.SendType == model.ClusterSendReliable {
							select {
							case rc.retryQueue <- msg:
								// Successfully re-queued
							default:
								rc.logger.Error("Retry queue full, dropping message",
									mlog.String("event", string(msg.Event)))
							}
						}
					}

					cancel()
				default:
					// No more messages in queue
					break retryLoop
				}
			}
		}
	}
}

// ClusterInterface implementation methods

// StartInterNodeCommunication starts the cluster communication
func (rc *RedisCluster) StartInterNodeCommunication() {
	if err := rc.Start(); err != nil {
		rc.logger.Error("Failed to start cluster communication", mlog.Err(err))
	}
}

// StopInterNodeCommunication stops the cluster communication
func (rc *RedisCluster) StopInterNodeCommunication() {
	rc.Stop()
}

// RegisterClusterMessageHandler registers a handler for a cluster event
func (rc *RedisCluster) RegisterClusterMessageHandler(event model.ClusterEvent, handler einterfaces.ClusterMessageHandler) {
	rc.handlersMutex.Lock()
	defer rc.handlersMutex.Unlock()
	rc.eventHandlers[event] = append(rc.eventHandlers[event], handler)
}

// GetClusterId returns the ID of this cluster node
func (rc *RedisCluster) GetClusterId() string {
	return rc.nodeID
}

// IsLeader returns whether this node is the cluster leader
func (rc *RedisCluster) IsLeader() bool {
	return rc.isLeader.Load()
}

// GetMyClusterInfo returns information about this cluster node
func (rc *RedisCluster) GetMyClusterInfo() *model.ClusterInfo {
	return &model.ClusterInfo{
		Id:      rc.nodeID,
		Version: model.CurrentVersion,
	}
}

// GetClusterInfos returns information about all nodes in the cluster
func (rc *RedisCluster) GetClusterInfos() []*model.ClusterInfo {
	ctx, cancel := context.WithTimeout(rc.ctx, 5*time.Second)
	defer cancel()

	pattern := fmt.Sprintf("%s:*", redisClusterNodesKey)
	keys, err := rc.redisClient.Keys(ctx, pattern).Result()
	if err != nil {
		rc.logger.Error("Failed to get cluster nodes", mlog.Err(err))
		return nil
	}

	var infos []*model.ClusterInfo
	for _, key := range keys {
		// Skip sync keys
		if bytes.Contains([]byte(key), []byte(":sync:")) {
			continue
		}

		data, err := rc.redisClient.Get(ctx, key).Result()
		if err != nil {
			rc.logger.Debug("Failed to get node info",
				mlog.String("key", key),
				mlog.Err(err))
			continue
		}

		var info model.ClusterInfo
		if err := json.Unmarshal([]byte(data), &info); err != nil {
			rc.logger.Debug("Failed to unmarshal node info",
				mlog.String("key", key),
				mlog.Err(err))
			continue
		}
		infos = append(infos, &info)
	}

	rc.logger.Debug("Got cluster information",
		mlog.Int("node_count", len(infos)),
		mlog.String("node_id", rc.nodeID))

	return infos
}

// SendClusterMessage sends a message to all nodes in the cluster
func (rc *RedisCluster) SendClusterMessage(msg *model.ClusterMessage) {
	// Initialize props if needed
	if msg.Props == nil {
		msg.Props = make(map[string]string)
	}

	// Add source node ID and message ID for tracking
	msg.Props["source_node_id"] = rc.nodeID
	msg.Props["msg_id"] = model.NewId()

	data, err := json.Marshal(msg)
	if err != nil {
		rc.logger.Error("Failed to marshal cluster message",
			mlog.Err(err),
			mlog.String("node_id", rc.nodeID))
		return
	}

	ctx, cancel := context.WithTimeout(rc.ctx, 2*time.Second)
	defer cancel()

	// Use specific channel for plugin events
	channel := redisClusterChannel
	if msg.Event == model.ClusterEventPluginEvent {
		channel = redisClusterEventChannel
	}

	// Handle reliable message delivery
	if rc.reliableEnabled && msg.SendType == model.ClusterSendReliable {
		key := fmt.Sprintf("%s:%s:%s", redisClusterReliablePrefix, string(msg.Event), msg.Props["msg_id"])

		// Store message in Redis for reliable delivery
		if err := rc.redisClient.Set(ctx, key, string(data), clusterReliableMsgTTL).Err(); err != nil {
			rc.logger.Error("Failed to store reliable message",
				mlog.Err(err),
				mlog.String("node_id", rc.nodeID),
				mlog.String("msg_id", msg.Props["msg_id"]))
		}

		// Add to list of reliable messages to process
		listKey := fmt.Sprintf("%s:list", redisClusterReliablePrefix)
		if err := rc.redisClient.RPush(ctx, listKey, key).Err(); err != nil {
			rc.logger.Error("Failed to add to reliable message list",
				mlog.Err(err),
				mlog.String("node_id", rc.nodeID))
		}

		// Ensure list has TTL
		rc.redisClient.Expire(ctx, listKey, clusterReliableMsgTTL)
	}

	// Use pipeline for multiple operations
	pipe := rc.redisClient.Pipeline()

	// Publish message
	pipe.Publish(ctx, channel, string(data))

	// Update sync version
	syncVer := rc.syncVersion.Add(1)
	pipe.Set(ctx, fmt.Sprintf("%s:sync:%s", redisClusterNodesKey, rc.nodeID), syncVer, clusterNodeTTL)

	_, err = pipe.Exec(ctx)
	if err != nil {
		rc.logger.Error("Failed to publish cluster message",
			mlog.Err(err),
			mlog.String("event", string(msg.Event)),
			mlog.String("node_id", rc.nodeID))

		// Add to retry queue
		if msg.SendType == model.ClusterSendReliable {
			select {
			case rc.retryQueue <- msg:
				rc.logger.Debug("Added message to retry queue",
					mlog.String("event", string(msg.Event)),
					mlog.String("msg_id", msg.Props["msg_id"]))
			default:
				rc.logger.Error("Retry queue full, dropping message",
					mlog.String("event", string(msg.Event)))
			}
		}
	}
}

// SendClusterMessageToNode sends a message to a specific node in the cluster
func (rc *RedisCluster) SendClusterMessageToNode(nodeID string, msg *model.ClusterMessage) error {
	// In Redis implementation, all messages are broadcasted
	rc.SendClusterMessage(msg)
	return nil
}

// Additional ClusterInterface method implementations

// GetClusterStats returns statistics about the cluster
func (rc *RedisCluster) GetClusterStats(ctx request.CTX) ([]*model.ClusterStats, *model.AppError) {
	nodes := rc.GetClusterInfos()
	stats := make([]*model.ClusterStats, 0, len(nodes))

	for _, node := range nodes {
		stat := &model.ClusterStats{
			Id: node.Id,
		}
		stats = append(stats, stat)
	}

	return stats, nil
}

// GetLogs returns logs from the cluster (not implemented)
func (rc *RedisCluster) GetLogs(ctx request.CTX, page, perPage int) ([]string, *model.AppError) {
	// Not implemented for Redis cluster
	return []string{}, nil
}

// GetPluginStatuses returns plugin statuses (not implemented)
func (rc *RedisCluster) GetPluginStatuses() (model.PluginStatuses, *model.AppError) {
	// Not implemented for Redis cluster
	return model.PluginStatuses{}, nil
}

// ConfigChanged handles configuration changes
func (rc *RedisCluster) ConfigChanged(old, new *model.Config, sendToOtherServer bool) *model.AppError {
	// Nothing to do for config changes
	return nil
}

// GenerateSupportPacket generates a support packet with cluster information
func (rc *RedisCluster) GenerateSupportPacket(ctx request.CTX, opts *model.SupportPacketOptions) (map[string][]model.FileData, error) {
	result := make(map[string][]model.FileData)

	// Add cluster information
	clusterInfo := &model.ClusterInfo{
		Id:      rc.nodeID,
		Version: model.CurrentVersion,
	}

	infoBytes, err := json.Marshal(clusterInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal cluster info: %w", err)
	}

	result["cluster_info"] = []model.FileData{{
		Filename: "cluster_info.json",
		Body:     infoBytes,
	}}

	// Add node status
	nodes := rc.GetClusterInfos()
	nodesBytes, err := json.Marshal(nodes)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal nodes info: %w", err)
	}

	result["cluster_nodes"] = []model.FileData{{
		Filename: "cluster_nodes.json",
		Body:     nodesBytes,
	}}

	return result, nil
}

// GetWSQueues returns WebSocket queues (not implemented)
func (rc *RedisCluster) GetWSQueues(userID, connectionID string, seqNum int64) (map[string]*model.WSQueues, error) {
	// For Redis implementation, we don't track WebSocket queues per node
	return map[string]*model.WSQueues{}, nil
}

// QueryLogs queries logs from the cluster (not implemented)
func (rc *RedisCluster) QueryLogs(rctx request.CTX, page, perPage int) (map[string][]string, *model.AppError) {
	// Not implemented for Redis cluster
	return map[string][]string{}, nil
}

// WebConnCountForUser returns the number of WebSocket connections for a user (not implemented)
func (rc *RedisCluster) WebConnCountForUser(userID string) (int, *model.AppError) {
	// For Redis implementation, we don't track connection counts per node
	return 0, nil
}

// NotifyMsg is a no-op for the Redis implementation
func (rc *RedisCluster) NotifyMsg(buf []byte) {
	// Not used in Redis implementation
}

// HealthScore returns a health score for the cluster
func (rc *RedisCluster) HealthScore() int {
	health := rc.CheckHealth()
	score := 0

	if !health.RedisConnected {
		score += 40
	}
	if !health.IsReady {
		score += 30
	}
	if len(health.Errors) > 0 {
		score += 20
	}
	if health.MessageQueueSize > maxClusterBatchSize*2 {
		score += 10
	}

	return score
}

// SetReady marks the cluster as ready
func (rc *RedisCluster) SetReady() {
	rc.isReady.Store(true)
	rc.logger.Info("Redis cluster node is ready", mlog.String("node_id", rc.nodeID))
}

// CheckHealth returns health information about the cluster
func (rc *RedisCluster) CheckHealth() *RedisClusterHealth {
	health := &RedisClusterHealth{
		IsLeader:       rc.IsLeader(),
		IsReady:        rc.isReady.Load(),
		LastSyncTime:   time.Unix(0, rc.lastSyncTimestamp.Load()),
		RedisConnected: true,
	}

	// Check Redis connection
	ctx, cancel := context.WithTimeout(rc.ctx, 2*time.Second)
	defer cancel()

	if err := rc.redisClient.Ping(ctx).Err(); err != nil {
		health.RedisConnected = false
		health.Errors = append(health.Errors, fmt.Errorf("redis connection error: %w", err))
	}

	// Get message queue size
	health.MessageQueueSize = len(rc.messageBuffer)

	// Get cluster node count
	nodes := rc.GetClusterInfos()
	health.NodeCount = len(nodes)

	// Check if we're in sync with other nodes
	if health.IsLeader {
		if err := rc.verifyClusterSync(); err != nil {
			health.Errors = append(health.Errors, fmt.Errorf("cluster sync error: %w", err))
		}
	}

	return health
}

// BroadcastWebSocketEvent broadcasts WebSocket events to all nodes
func (rc *RedisCluster) BroadcastWebSocketEvent(event *model.WebSocketEvent) {
	// Skip if the event is already from another cluster node
	if event.IsFromCluster() {
		return
	}

	// Apply broadcast hooks if defined
	if event.GetBroadcast() != nil && len(event.GetBroadcast().BroadcastHooks) > 0 {
		ev, hooks, hookArgs := event.WithoutBroadcastHooks()

		for i, hookID := range hooks {
			if hook, ok := rc.broadcastHooks[hookID]; ok {
				var args map[string]any
				if i < len(hookArgs) {
					args = hookArgs[i]
				}

				// Create a HookedWebSocketEvent to pass to the hook
				hookedEvent := MakeHookedWebSocketEvent(ev)

				// Call the Process method
				if err := hook.Process(hookedEvent, nil, args); err != nil {
					rc.logger.Error("Failed to process broadcast hook",
						mlog.String("hook_id", hookID),
						mlog.Err(err))
					continue
				}

				// Get the processed event
				ev = hookedEvent.Event()
			}
		}

		event = ev
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
		SendType: model.ClusterSendBestEffort,
	}

	// Determine if this should be sent reliably based on event type
	needsReliableDelivery := false

	switch event.EventType() {
	case model.WebsocketEventPosted,
		model.WebsocketEventPostEdited,
		model.WebsocketEventPostDeleted,
		model.WebsocketEventDirectAdded,
		model.WebsocketEventGroupAdded,
		model.WebsocketEventAddedToTeam,
		model.WebsocketEventLeaveTeam,
		model.WebsocketEventUpdateTeam,
		model.WebsocketEventUserAdded,
		model.WebsocketEventUserUpdated,
		model.WebsocketEventStatusChange,
		model.WebsocketEventHello,
		model.WebsocketEventChannelUpdated,
		model.WebsocketEventChannelCreated,
		model.WebsocketEventChannelDeleted:
		needsReliableDelivery = true
	}

	// Override reliable flag from broadcast
	if event.GetBroadcast() != nil && event.GetBroadcast().ReliableClusterSend {
		needsReliableDelivery = true
	}

	if needsReliableDelivery {
		msg.SendType = model.ClusterSendReliable
	}

	// Send to other nodes via Redis
	rc.SendClusterMessage(msg)
}

// Publish sends a WebSocket event to all nodes
func (rc *RedisCluster) Publish(event *model.WebSocketEvent) {
	// Skip if the event is already from another cluster node
	if event.IsFromCluster() {
		return
	}

	// Mark the event as coming from cluster
	event = event.SetFromCluster(true)

	// Set the node ID in the broadcast if it exists
	if event.GetBroadcast() != nil {
		event = event.SetBroadcast(event.GetBroadcast())
		event.GetBroadcast().ConnectionId = rc.nodeID
	}

	// Local broadcast first for this node
	rc.platformService.PublishSkipClusterSend(event)

	// Prepare cluster message for other nodes
	data, err := event.ToJSON()
	if err != nil {
		rc.logger.Error("Failed to marshal WebSocket event", mlog.Err(err))
		return
	}

	// Create a cluster message to send to other nodes
	msg := &model.ClusterMessage{
		Event:    model.ClusterEventPublish,
		Data:     data,
		SendType: model.ClusterSendBestEffort,
	}

	// Check if this needs reliable delivery
	if event.EventType() == model.WebsocketEventPosted ||
		event.EventType() == model.WebsocketEventPostEdited ||
		(event.GetBroadcast() != nil && event.GetBroadcast().ReliableClusterSend) {
		msg.SendType = model.ClusterSendReliable
	}

	// Send to all other nodes
	rc.SendClusterMessage(msg)
}
