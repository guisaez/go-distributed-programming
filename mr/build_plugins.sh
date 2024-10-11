#!/bin/bash

# Base directory to search for subdirectories
BASE_DIR="./mr_apps"
OUTPUT_DIR="./cmd/mr_worker"

# Create the output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Iterate over all subdirectories
for dir in "$BASE_DIR"/*/; do
    # Check if the directory contains any Go files
    if ls "$dir"*.go &> /dev/null; then
        # Iterate over each Go file in the directory
        for go_file in "$dir"*.go; do
            # Get the base name of the Go file (without extension)
            file_name=$(basename "$go_file" .go)
            output_file="$OUTPUT_DIR/${file_name}.so"  # Specify the output file name
            
            echo "Building Go file: $go_file"
            # Run go build for each Go file
            go build -buildmode=plugin -o "$output_file" "$go_file"
            if [ $? -eq 0 ]; then
                echo "Build succeeded for $go_file -> $output_file"
            else
                echo "Build failed for $go_file"
            fi
        done
    else
        echo "No Go files found in directory: $dir"
    fi
done