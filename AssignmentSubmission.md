# **Challenges faced and how they were overcome:**

## Challenge 1: Port Conflict with MySQL (3306)
**Problem:** Docker container couldn't start due to port 3306 already being in use.  
**Solution:** Identified the process using the port with system tools and terminated it to free up the port.

## Challenge 2: NPM Dependencies Installation Failure
**Problem:** Network connectivity issues when fetching Git dependencies over SSH.  
**Solution:** Modified SSH configuration for GitHub and cleared npm cache to resolve connection issues.

