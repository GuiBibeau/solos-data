# shellcheck shell=sh disable=SC1090,SC2034
# Sourced by backup.sh and restore-check.sh: sets $dest and gives rclone the R2 remote through
# its environment only (no rclone config file, nothing secret on a command line or in a log).
# SOLOS_BACKUP_DEST (any rclone path, e.g. a local directory) bypasses R2 for tests.
env_file=${SOLOS_BACKUP_ENV:-$HOME/.config/solos-data/backup.env}
if [ -n "${SOLOS_BACKUP_DEST:-}" ]; then
  dest=$SOLOS_BACKUP_DEST
else
  if [ ! -r "$env_file" ]; then
    echo '{"event":"backup_credentials","state":"missing"}'
    exit 1
  fi
  set -a
  . "$env_file"
  set +a
  : "${R2_ACCOUNT_ID:?}" "${R2_BUCKET:?}" "${R2_ACCESS_KEY_ID:?}" "${R2_SECRET_ACCESS_KEY:?}"
  export RCLONE_CONFIG_R2_TYPE=s3 RCLONE_CONFIG_R2_PROVIDER=Cloudflare
  export RCLONE_CONFIG_R2_ENDPOINT="https://$R2_ACCOUNT_ID.r2.cloudflarestorage.com"
  export RCLONE_CONFIG_R2_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID"
  export RCLONE_CONFIG_R2_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY"
  export RCLONE_CONFIG_R2_NO_CHECK_BUCKET=true
  dest="r2:$R2_BUCKET"
fi
export RCLONE_CONFIG=
rclone=${RCLONE:-$HOME/.local/bin/rclone}
