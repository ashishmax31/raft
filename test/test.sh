#!/bin/bash

# Define the test command
TEST_COMMAND="go test -run ^TestBackup2B"
# Initialize iteration counter
iterations=0 

# Run the test in a loop until it returns a non-zero exit code
while $TEST_COMMAND; [ $? -eq 0 ]; do
  ((iterations++))
  echo "Iteration $iterations: Test passed, running again..."
done

echo "Test failed after $iterations iterations with exit code $?. Exiting."
