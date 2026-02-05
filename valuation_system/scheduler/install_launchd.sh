#!/bin/bash
# =============================================================================
# Install launchd jobs for the Agentic Valuation System
# Run once to set up scheduled tasks on macOS
#
# Usage: bash install_launchd.sh [install|uninstall|status]
# =============================================================================

PLIST_DIR="$HOME/Library/LaunchAgents"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

PLISTS=(
    "com.valuation.hourly"
    "com.valuation.daily"
    "com.valuation.social"
    "com.valuation.regression"
)

install() {
    echo "Installing launchd jobs..."
    mkdir -p "$PLIST_DIR"
    mkdir -p "$SCRIPT_DIR/../logs"

    for plist in "${PLISTS[@]}"; do
        src="$SCRIPT_DIR/${plist}.plist"
        dst="$PLIST_DIR/${plist}.plist"

        if [ -f "$src" ]; then
            # Unload if already loaded
            launchctl unload "$dst" 2>/dev/null

            # Copy plist
            cp "$src" "$dst"

            # Load
            launchctl load "$dst"
            echo "  Loaded: $plist"
        else
            echo "  WARNING: $src not found, skipping"
        fi
    done

    echo ""
    echo "All jobs installed. Check status with: bash $0 status"
    echo ""
    echo "Schedule:"
    echo "  Hourly:     Every hour (news scan + driver update)"
    echo "  Daily:      20:00 (valuation refresh + alerts)"
    echo "  Social:     08:00 (pre-market social media posts)"
    echo "  Regression: 06:00 (automated test suite + email report)"
}

uninstall() {
    echo "Uninstalling launchd jobs..."
    for plist in "${PLISTS[@]}"; do
        dst="$PLIST_DIR/${plist}.plist"
        if [ -f "$dst" ]; then
            launchctl unload "$dst" 2>/dev/null
            rm "$dst"
            echo "  Removed: $plist"
        fi
    done
    echo "All jobs uninstalled."
}

status() {
    echo "Launchd job status:"
    echo ""
    for plist in "${PLISTS[@]}"; do
        dst="$PLIST_DIR/${plist}.plist"
        if [ -f "$dst" ]; then
            result=$(launchctl list | grep "$plist" 2>/dev/null)
            if [ -n "$result" ]; then
                echo "  $plist: LOADED ($result)"
            else
                echo "  $plist: INSTALLED but NOT LOADED"
            fi
        else
            echo "  $plist: NOT INSTALLED"
        fi
    done
}

case "$1" in
    install)
        install
        ;;
    uninstall)
        uninstall
        ;;
    status)
        status
        ;;
    *)
        echo "Usage: $0 {install|uninstall|status}"
        exit 1
        ;;
esac
