#!/bin/bash

echo "Creating a patch containing all changes from current branch to master"

git diff master > ipv6.patch