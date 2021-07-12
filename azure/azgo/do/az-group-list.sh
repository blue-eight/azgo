az group list | jq -c '.[] | [leaf_paths as $path | {"key": $path | join("_"), "value": getpath($path)}] | from_entries | .id_ = .id | del(.id)'
