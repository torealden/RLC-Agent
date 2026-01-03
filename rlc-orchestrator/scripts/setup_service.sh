#!/bin/bash
# ==============================================================================
# RLC Orchestrator - Systemd Service Setup
# ==============================================================================
# This script sets up the orchestrator to run as a systemd service.
# This means it will:
# - Start automatically when the server boots
# - Restart automatically if it crashes
# - Be manageable via standard systemctl commands
#
# Run this script once during initial setup:
#   sudo bash scripts/setup_service.sh
#
# After setup, use these commands to manage the service:
#   sudo systemctl start rlc-orchestrator     # Start the service
#   sudo systemctl stop rlc-orchestrator      # Stop the service
#   sudo systemctl restart rlc-orchestrator   # Restart the service
#   sudo systemctl status rlc-orchestrator    # Check status
#   journalctl -u rlc-orchestrator -f         # View logs in real-time
# ==============================================================================

set -e  # Exit on error

# Configuration - modify these for your setup
SERVICE_NAME="rlc-orchestrator"
SERVICE_USER="rlc"                    # User to run the service as
PROJECT_DIR="/home/${SERVICE_USER}/rlc-orchestrator"
PYTHON_PATH="/home/${SERVICE_USER}/.venv/bin/python"
WORKING_DIR="${PROJECT_DIR}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Setting up RLC Orchestrator as a systemd service${NC}"
echo "=============================================="

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}Error: This script must be run as root (use sudo)${NC}"
    exit 1
fi

# Check if the project directory exists
if [ ! -d "$PROJECT_DIR" ]; then
    echo -e "${YELLOW}Warning: Project directory not found at ${PROJECT_DIR}${NC}"
    echo "Please update the PROJECT_DIR variable in this script."
    exit 1
fi

# Check if the user exists
if ! id "$SERVICE_USER" &>/dev/null; then
    echo -e "${YELLOW}Creating service user: ${SERVICE_USER}${NC}"
    useradd -m -s /bin/bash "$SERVICE_USER"
fi

# Create the systemd service file
cat > /etc/systemd/system/${SERVICE_NAME}.service << EOF
[Unit]
Description=RLC Orchestrator - AI-Powered Business Automation
After=network.target
StartLimitIntervalSec=0

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_USER}
WorkingDirectory=${WORKING_DIR}
ExecStart=${PYTHON_PATH} main.py
Restart=always
RestartSec=5

# Environment
Environment="PYTHONUNBUFFERED=1"
EnvironmentFile=-${PROJECT_DIR}/config/.env

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=${SERVICE_NAME}

# Security hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=read-only
PrivateTmp=true
ReadWritePaths=${PROJECT_DIR}/data ${PROJECT_DIR}/logs

# Resource limits
MemoryMax=512M
CPUQuota=50%

[Install]
WantedBy=multi-user.target
EOF

echo -e "${GREEN}Created systemd service file${NC}"

# Set permissions on project directory
chown -R ${SERVICE_USER}:${SERVICE_USER} ${PROJECT_DIR}
chmod -R 750 ${PROJECT_DIR}

# Reload systemd to recognize the new service
systemctl daemon-reload

echo -e "${GREEN}Reloaded systemd daemon${NC}"

# Enable the service to start on boot
systemctl enable ${SERVICE_NAME}

echo -e "${GREEN}Enabled service to start on boot${NC}"

# Print next steps
echo ""
echo -e "${GREEN}Setup complete!${NC}"
echo ""
echo "Next steps:"
echo "  1. Edit the environment file: ${PROJECT_DIR}/config/.env"
echo "  2. Start the service: sudo systemctl start ${SERVICE_NAME}"
echo "  3. Check status: sudo systemctl status ${SERVICE_NAME}"
echo "  4. View logs: journalctl -u ${SERVICE_NAME} -f"
echo ""
echo "Useful commands:"
echo "  sudo systemctl start ${SERVICE_NAME}     # Start"
echo "  sudo systemctl stop ${SERVICE_NAME}      # Stop"
echo "  sudo systemctl restart ${SERVICE_NAME}   # Restart"
echo "  sudo systemctl status ${SERVICE_NAME}    # Status"
