#!/bin/bash

# Remove files in ../final
find ../final -type f -delete

# Remove files in ../intermediate
find ../intermediate -type f -delete

# Remove files in ../maps
find ../maps -type f -delete

echo "Files removed from ../final, ../intermediate, and ../maps"
