#!/bin/bash

# Create and enter build directory
mkdir -p build
cd build

# Configure CMake in Debug mode with Clang 14
cmake \
  -DCMAKE_C_COMPILER=/opt/homebrew/opt/llvm@14/bin/clang-14 \
  -DCMAKE_CXX_COMPILER=/opt/homebrew/opt/llvm@14/bin/clang++ \
  -DCMAKE_BUILD_TYPE=Debug \
  ..

# Array of test names
tests=(
    "txn_timestamp_test"
    "txn_scan_test"
)

# Build and run each test
for test in "${tests[@]}"; do
    echo "Building $test..."
    if ! make "$test" -j$(nproc); then
        echo "Failed to build $test"
        exit 1
    fi
    
    echo "Running $test..."
    if ! "./test/$test"; then
        echo "$test failed!"
        exit 1
    fi
    echo "$test completed successfully"
    echo "------------------------"
done

echo "All tests completed successfully!"