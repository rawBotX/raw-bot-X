#!/bin/bash

# Navigate to the script's directory to ensure relative paths work correctly
cd "$(dirname "$0")" || exit 1

echo "========================================================================"
echo "== This script will set file permissions for your Telegram bot.       =="
echo "== It targets config.env.gpg, *.json, and *.txt files, ensuring     =="
echo "== that only the owner can read and write these files.              =="
echo "========================================================================"
echo

# Function to check and set permissions for a single file or pattern
# Arguments: $1 = file/pattern, $2 = desired_permission_string (e.g., "600")
set_permissions() {
    local target="$1"
    local desired_perm_numeric="$2" # e.g., 600

    if [[ "$target" == *\** ]]; then
        local files_found=0
        shopt -s nullglob
        for file in $target; do
            files_found=1
            set_permissions "$file" "$desired_perm_numeric"
        done
        shopt -u nullglob

        if [ "$files_found" -eq 0 ]; then
            echo "INFO: No files found matching pattern '$target'. Skipping."
            echo
        fi
        return
    fi

    if [ ! -e "$target" ]; then
        echo "WARNING: File '$target' not found. Skipping."
        echo
        return
    fi

    echo "Processing permissions for: $target"
    current_perm_octal=$(stat -c "%a" "$target")

    if [ "$current_perm_octal" == "$desired_perm_numeric" ]; then
        echo "  - Permissions are already correctly set to $desired_perm_numeric (-rw-------)."
    else
        echo "  - Current permissions: $current_perm_octal. Desired: $desired_perm_numeric."
        chmod "$desired_perm_numeric" "$target"
        if [ $? -eq 0 ]; then
            echo "  - Permissions successfully updated to $desired_perm_numeric (-rw-------)."
        else
            echo "  - ERROR: Failed to update permissions for '$target'."
        fi
    fi
    echo
}

# --- Files and patterns to process ---

# Process config.env.gpg specifically (if it exists)
# If config.env also needs 600, it should be handled by a *.env pattern or explicitly
# Based on your last Windows script, *.env files were processed.
# If you only want config.env.gpg and not other .env files, this is fine.
# If you also want config.env to have its permissions set (before potential deletion), add:
# set_permissions "*.env" "600"
# For now, sticking to your provided list:
set_permissions "config.env.gpg" "600" # This will only act if config.env.gpg exists

# All .json files
set_permissions "*.json" "600"

# All .txt files
set_permissions "*.txt" "600"

echo "========================================================================"
echo "== All permission checks and updates are complete.                    =="
echo "========================================================================"
echo

# --- Check for config.env deletion ---
delete_config_env_flag=false
if [ -e "config.env.gpg" ]; then
    if [ -e "config.env" ]; then
        echo "INFO: 'config.env.gpg' and 'config.env' found."
        echo "The unencrypted 'config.env' file will be deleted"
        echo "when you press a key to close this window."
        delete_config_env_flag=true
    else
        echo "INFO: 'config.env.gpg' found, but 'config.env' does not exist. No deletion needed."
    fi
else
    echo "INFO: 'config.env.gpg' not found. 'config.env' will not be deleted."
fi
echo
echo "Press any key to finalize and close this window..."
read -n 1 -s -r # -p prompt removed to match the Windows version's final pause behavior

# --- Perform deletion if flagged ---
if [ "$delete_config_env_flag" = true ]; then
    echo # Newline before deletion messages
    echo "Deleting 'config.env' now..."
    rm "config.env"
    if [ $? -eq 0 ]; then
        echo "  - SUCCESS: 'config.env' has been deleted."
    else
        echo "  - ERROR: Failed to delete 'config.env'. It might be in use or protected."
    fi
fi

echo # Add a final newline for cleaner terminal output