"""ports and Urls for system """

# Port Configuration
ORCHESTRATOR_PORT = 5000
CAPTURE_PROCESS_AGENT_PORT = 5001
VALIDATION_AGENT_PORT = 5002
FILEMANAGER_AGENT_PORT = 5003

# Agent URLs
AGENT_URLS = {
    'orchestrator': f'http://localhost:{ORCHESTRATOR_PORT}',
    'capture': f'http://localhost:{CAPTURE_PROCESS_AGENT_PORT}',
    'validation': f'http://localhost:{VALIDATION_AGENT_PORT}',
    'filemanager': f'http://localhost:{FILEMANAGER_AGENT_PORT}'
}

# Timeout settings (seconds)
API_TIMEOUT = 10
AGENT_COMPLETION_TIMEOUT = 300
POLLING_INTERVAL = 2

# Logging
LOG_FILE = 'multi_agent_system.log'
LOG_LEVEL = 'INFO'

