#!/bin/bash
# deploy.sh - Package and publish a plugin to an OpenMotics gateway.
#
# Usage:
#   ./deploy.sh <plugin> <ip/hostname> <username> <password>
#
# Environment variables (alternative to positional args):
#   OM_PLUGIN, OM_HOST, OM_USER, OM_PASS
#
# Example:
#   ./deploy.sh ha-mqtt-discovery 192.168.168.161 myuser mypass
#   OM_PASS=mypass ./deploy.sh ha-mqtt-discovery 192.168.168.161 myuser

set -eu

plugin=${1:-${OM_PLUGIN:-}}
host=${2:-${OM_HOST:-}}
username=${3:-${OM_USER:-}}
password=${4:-${OM_PASS:-}}

if [[ -z "$plugin" || -z "$host" || -z "$username" || -z "$password" ]]; then
  echo "Usage: ./deploy.sh <plugin> <ip/hostname> <username> <password>"
  echo "       (or set OM_PLUGIN, OM_HOST, OM_USER, OM_PASS environment variables)"
  exit 1
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Packaging ${plugin}..."
"${script_dir}/package.sh" "$plugin"

if [ "$(uname -s)" = "Darwin" ]; then
  sedcmd='sed -E'
else
  sedcmd='sed -r'
fi

version=$(grep -m1 'version' "${script_dir}/${plugin}/main.py" | cut -d'"' -f2 | cut -d"'" -f2)
package="${script_dir}/${plugin}_${version}.tgz"

if [[ ! -f "$package" ]]; then
  echo "ERROR: Expected package not found: $package"
  exit 1
fi

checksum=$(md5sum "$package" | cut -d ' ' -f 1)

echo "==> Logging in to ${host}..."
login=$(curl -sk -X GET "https://${host}/login?username=${username}&password=${password}")
success=$(echo "$login" | $sedcmd 's/.*"success": *([a-z]+).*/\1/')

if [[ "$success" != "true" ]]; then
  echo "ERROR: Login failed: $login"
  exit 1
fi

token=$(echo "$login" | $sedcmd 's/.*"token": *"([a-z0-9]+)".*/\1/')

echo "==> Installing ${plugin}_${version}.tgz on ${host}..."
result=$(curl -sk \
  --form "package_data=@${package}" \
  --form "md5=${checksum}" \
  --form "token=${token}" \
  -X POST "https://${host}/install_plugin")

success=$(echo "$result" | $sedcmd 's/.*"success": *([a-z]+).*/\1/')

if [[ "$success" = "true" ]]; then
  echo "==> Deploy succeeded: ${plugin} v${version} -> ${host}"
else
  msg=$(echo "$result" | $sedcmd 's/.*"msg": *"([^"]+).*/\1/')
  echo "ERROR: Deploy failed: $msg"
  exit 1
fi
