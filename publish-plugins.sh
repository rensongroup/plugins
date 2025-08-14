#!/bin/bash

# Publish Plugins Script
# Converts the GitHub Actions workflow to a standalone bash script

set -e  # Exit on any error

# Default values
PLUGINS=""
ENVIRONMENT="dev"
VERBOSE=false

# Function to show usage
show_usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Options:
    -p, --plugins PLUGINS       Comma-separated list of plugin names to build and publish (leave empty for all)
    -e, --environment ENV       Target environment (dev, stg, prd) [default: dev]
    -v, --version VERSION       Override version for all plugins (optional)
    --verbose                   Enable verbose output
    -h, --help                  Show this help message

Environment Variables:
    CLOUD_CICD_TOKEN_DEV       API token for dev environment
    CLOUD_CICD_TOKEN_STG       API token for stg environment  
    CLOUD_CICD_TOKEN_PRD       API token for prd environment

Examples:
    $0 --environment dev
    $0 --plugins "bacnet,modbus" --environment stg
    $0 --plugins "bacnet" --version "1.2.3" --environment prd
EOF
}

# Function for logging
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >&2
}

log_verbose() {
    if [ "$VERBOSE" = true ]; then
        echo "[VERBOSE] $1" >&2
    fi
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -p|--plugins)
            PLUGINS="$2"
            shift 2
            ;;
        -e|--environment)
            ENVIRONMENT="$2"
            shift 2
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Validate environment
if [[ ! "$ENVIRONMENT" =~ ^(dev|stg|prd)$ ]]; then
    echo "Error: Invalid environment '$ENVIRONMENT'. Must be one of: dev, stg, prd"
    exit 1
fi

# Set API base URL
API_BASE_URL="https://${ENVIRONMENT}-api.openmotics-internal.com:8443/api/internal/plugins/release_artifact"
# Get API token based on environment
case "$ENVIRONMENT" in
    "dev")
        API_TOKEN="$CLOUD_CICD_TOKEN_DEV"
        ;;
    "stg")
        API_TOKEN="$CLOUD_CICD_TOKEN_STG"
        ;;
    "prd")
        API_TOKEN="$CLOUD_CICD_TOKEN_PRD"
        ;;
esac

if [ -z "$API_TOKEN" ]; then
    echo "Error: API token not found. Please set environment variable CLOUD_CICD_TOKEN_${ENVIRONMENT^^}"
    exit 1
fi

HOST="${ENVIRONMENT}.openmotics.com"

log "Starting plugin build and publish process"
log "Environment: $ENVIRONMENT"
log "API Base URL: $API_BASE_URL"

# Function to discover plugins
discover_plugins() {
    log "Discovering plugins..."
    
    # Get list of directories with info.json files (these are plugins)
    all_plugins=$(find . -maxdepth 2 -name "info.json" -type f | cut -d'/' -f2 | sort)
    
    if [ -n "$PLUGINS" ]; then
        # Use specified plugins
        log "Using specified plugins: $PLUGINS"
        plugins_array=""
        IFS=',' read -ra PLUGIN_LIST <<< "$PLUGINS"
        for plugin in "${PLUGIN_LIST[@]}"; do
            plugin=$(echo "$plugin" | xargs) # trim whitespace
            if [ -d "$plugin" ] && [ -f "$plugin/info.json" ]; then
                plugins_array="$plugins_array $plugin"
                log_verbose "Found plugin: $plugin"
            else
                echo "Warning: Plugin '$plugin' not found or missing info.json"
            fi
        done
        discovered_plugins=$(echo $plugins_array | xargs)
    else
        # Use all discovered plugins
        discovered_plugins="$all_plugins"
        log "Using all discovered plugins"
    fi
    
    if [ -z "$discovered_plugins" ]; then
        log "No plugins found to process"
        exit 0
    fi
    
    log "Discovered plugins: $discovered_plugins"
    echo "$discovered_plugins"
}

# Function to get plugin version
get_plugin_version() {
    local plugin_name="$1"
    local version=""
    
    # Get version from plugin's info.json
    if [ -f "$plugin_name/info.json" ]; then
        if command -v jq &> /dev/null; then
            version=$(jq -r '.version // empty' "$plugin_name/info.json")
        else
            # Fallback without jq
            version=$(grep -o '"version"[[:space:]]*:[[:space:]]*"[^"]*"' "$plugin_name/info.json" | cut -d'"' -f4)
        fi
    fi
    
    # If not found in info.json, try main.py
    if [ -z "$version" ] && [ -f "$plugin_name/main.py" ]; then
        version=$(grep "version.*=" "$plugin_name/main.py" | cut -d '"' -f 2 | cut -d "'" -f 2 | head -n 1)
    fi
    
    
    if [ -z "$version" ]; then
        echo "Error: Could not determine version for plugin $plugin_name"
        exit 1
    fi
    
    echo "$version"
}

# Function to validate plugin structure
validate_plugin() {
    local plugin_name="$1"
    local version="$2"
    
    log_verbose "Validating plugin structure for $plugin_name"
    
    if [ ! -d "$plugin_name" ]; then
        echo "Error: Plugin directory '$plugin_name' not found"
        exit 1
    fi
    
    if [ ! -f "$plugin_name/info.json" ]; then
        echo "Error: info.json not found in plugin '$plugin_name'"
        exit 1
    fi
    
    if [ ! -f "$plugin_name/main.py" ]; then
        echo "Error: main.py not found in plugin '$plugin_name'"
        exit 1
    fi
    
    # Validate version format
    if ! [[ $version =~ ^([0-9]+\.)([0-9]+\.)(\*|[0-9]+)$ ]]; then
        echo "Error: Invalid version format '$version' for plugin '$plugin_name'"
        exit 1
    fi
    
    log_verbose "Plugin $plugin_name validation passed"
}

# Function to package plugin
package_plugin() {
    local plugin_name="$1"
    local version="$2"
    
    log "Packaging plugin $plugin_name version $version"
    
    if [ ! -x "package.sh" ]; then
        echo "Error: package.sh script not found or not executable"
        exit 1
    fi
    
    # Use the existing package.sh script
    ./package.sh "$plugin_name" >&2

    local artifact_name="${plugin_name}_${version}.tgz"
    local md5_file="${plugin_name}_${version}.md5"
    
    # Verify the files were created
    if [ ! -f "$artifact_name" ]; then
        echo "Error: Artifact file $artifact_name was not created"
        exit 1
    fi
    
    if [ ! -f "$md5_file" ]; then
        echo "Error: MD5 file $md5_file was not created"
        exit 1
    fi
    
    log_verbose "Package created successfully:"
    log_verbose "$(ls -la "$artifact_name" "$md5_file")"
    
    echo "$artifact_name:$md5_file"
}

# Function to upload plugin
upload_plugin() {
    local plugin_name="$1"
    local version="$2"
    local artifact_name="$3"
    local md5_file="$4"
    
    log "Publishing plugin $plugin_name version $version to $ENVIRONMENT environment"
    
    # Read MD5 checksum (handle both single and double space formats)
    local md5_checksum=$(awk '{print $1}' "$md5_file")
    
    # Read info.json content as string
    local info_json=$(cat "$plugin_name/info.json")
    
    log_verbose "Plugin info:"
    log_verbose "$info_json"
    log_verbose "MD5 checksum: $md5_checksum"
    
    # Prepare the curl command with proper error handling
    local response_file="response_${plugin_name}.json"
    local http_code
    
    http_code=$(curl -X POST \
        -w '%{http_code}' \
        -H "x-api-key: $API_TOKEN" \
        -H "Host: $HOST" \
        -F "name=$plugin_name" \
        -F "version=$version" \
        -F "info=$info_json" \
        -F "md5=$md5_checksum" \
        -F "artifact=@$artifact_name" \
        "$API_BASE_URL" \
        -o "$response_file" \
        --silent)
    
    log_verbose "HTTP Response Code: $http_code"
    
    if [ -f "$response_file" ]; then
        log_verbose "Response body:"
        log_verbose "$(cat "$response_file")"
    fi
    
    # Check if upload was successful
    if [ "$http_code" -ge 200 ] && [ "$http_code" -lt 300 ]; then
        log "✅ Successfully published plugin $plugin_name version $version to $ENVIRONMENT"
        # Clean up temporary files
        rm -f "$response_file"
        return 0
    else
        echo "❌ Failed to publish plugin $plugin_name version $version to $ENVIRONMENT"
        echo "HTTP Code: $http_code"
        if [ -f "$response_file" ]; then
            echo "Response:"
            cat "$response_file"
        fi
        # Clean up temporary files
        rm -f "$response_file"
        return 1
    fi
}

# Function to process a single plugin
process_plugin() {
    local plugin_name="$1"
    
    log "Processing plugin: $plugin_name"
    
    # Get version
    local version
    version=$(get_plugin_version "$plugin_name")
    log "Plugin $plugin_name version: $version"
    
    # Validate plugin
    validate_plugin "$plugin_name" "$version"
    
    # Package plugin
    local package_result
    package_result=$(package_plugin "$plugin_name" "$version")
    local artifact_name=$(echo "$package_result" | cut -d':' -f1)
    local md5_file=$(echo "$package_result" | cut -d':' -f2)
    
    # Upload plugin
    if upload_plugin "$plugin_name" "$version" "$artifact_name" "$md5_file"; then
        # Clean up package files after successful upload
        rm -f "$artifact_name" "$md5_file"
        return 0
    else
        return 1
    fi
}

# Main execution
main() {
    log "Plugin Build and Publish Script"
    log "================================"
    
    # Discover plugins
    plugins_to_process=$(discover_plugins)
    
    local success_count=0
    local failure_count=0
    local failed_plugins=""
    
    # Process each plugin
    for plugin in $plugins_to_process; do
        log ""
        log "--- Processing Plugin: $plugin ---"
        
        if process_plugin "$plugin"; then
            ((success_count++))
        else
            ((failure_count++))
            failed_plugins="$failed_plugins $plugin"
        fi
    done
    
    # Summary
    log ""
    log "================================"
    log "Build and Publish Summary"
    log "================================"
    log "Environment: $ENVIRONMENT"
    log "Requested plugins: ${PLUGINS:-All available plugins}"
    log ""
    log "Successful: $success_count"
    log "Failed: $failure_count"
    
    if [ $failure_count -gt 0 ]; then
        log "Failed plugins:$failed_plugins"
        exit 1
    else
        log "✅ All plugins were successfully built and published!"
        exit 0
    fi
}

# Check if script is being sourced or executed
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi