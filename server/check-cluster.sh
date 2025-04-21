#!/bin/bash

echo "Mattermost Cluster Status Check"
echo "=============================="

# Check if nodes are running
NODE1_RUNNING=false
NODE2_RUNNING=false

# Function to check if port is in use
is_port_in_use() {
  lsof -i :$1 >/dev/null 2>&1
  return $?
}

# Check node 1 (port 8065)
if is_port_in_use 8065; then
  NODE1_RUNNING=true
  echo "✓ Node 1 is running on port 8065"
else
  echo "✗ Node 1 is not running"
  echo "  To start: cd /home/sandip/mattermost && make run-server"
fi

# Check node 2 (port 8066)
if is_port_in_use 8066; then
  NODE2_RUNNING=true
  echo "✓ Node 2 is running on port 8066"
else
  echo "✗ Node 2 is not running"
  echo "  To start in a new terminal: cd /home/sandip/mattermost && MM_SERVICESETTINGS_LISTENADDRESS=:8066 MM_SERVICESETTINGS_SITEURL=http://localhost:8066 MM_FILESETTINGS_DIRECTORY=/tmp/mmdata2 make run-server"
fi

# Check Redis
if redis-cli ping >/dev/null 2>&1; then
  echo "✓ Redis is running"
else
  echo "✗ Redis is not running - this is required for cluster functionality"
  echo "  To start: redis-server --daemonize yes"
  
  # Also suggest using run-with-redis.sh script
  if [ -f "/home/sandip/mattermost/run-with-redis.sh" ]; then
    echo "  Or use: /home/sandip/mattermost/run-with-redis.sh to start Redis and Mattermost"
  fi
fi

# Display data directories
echo
echo "Data Directories:"
echo "- Node 1: /tmp/mmdata1"
echo "- Node 2: /tmp/mmdata2"

# If both nodes are running, check API status
if [ "$NODE1_RUNNING" = true ] && [ "$NODE2_RUNNING" = true ]; then
  echo
  echo "Testing API connectivity..."
  
  # Try to ping both nodes
  NODE1_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8065/api/v4/system/ping)
  NODE2_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8066/api/v4/system/ping)
  
  [ "$NODE1_STATUS" = "200" ] && echo "✓ Node 1 API responded with status 200" || echo "✗ Node 1 API returned status $NODE1_STATUS"
  [ "$NODE2_STATUS" = "200" ] && echo "✓ Node 2 API responded with status 200" || echo "✗ Node 2 API returned status $NODE2_STATUS"
  
  echo
  echo "Cluster seems to be running correctly!"
  echo
  echo "Access your nodes at:"
  echo "- Node 1: http://localhost:8065"
  echo "- Node 2: http://localhost:8066"
else
  echo
  echo "For easy cluster startup, run the following script:"
  echo "  /home/sandip/mattermost/run-with-redis.sh"
  echo "  Then answer 'y' when asked to start both nodes."
fi

# Log file locations
echo
echo "Log file locations:"
echo "- Node 1: /tmp/mmdata1/logs"
echo "- Node 2: /tmp/mmdata2/logs"
