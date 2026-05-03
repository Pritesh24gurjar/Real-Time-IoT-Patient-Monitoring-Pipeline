#!/usr/bin/env bash
# ============================================================================
# Stop Pipeline - EC2 / Linux Version
# ============================================================================

SESSION="pipeline"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "============================================================"
echo " Stopping Kafka Real-Time Pipeline"
echo "============================================================"
echo ""

# Kill tmux session
if tmux has-session -t "$SESSION" 2>/dev/null; then
    echo "Stopping tmux session: $SESSION..."
    tmux kill-session -t "$SESSION"
    echo "  tmux session stopped."
else
    echo "  No tmux session '$SESSION' found."
fi

# Stop Docker containers
cd "$PROJECT_DIR"
if docker-compose ps | grep -q "Up"; then
    echo "Stopping Docker containers..."
    docker-compose down
    echo "  Docker containers stopped."
else
    echo "  No running Docker containers found."
fi

echo ""
echo "All components stopped."
echo "==========================================================="