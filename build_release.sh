#!/bin/bash

# Create and enter build directory
mkdir -p build_release
cd build_release

# Configure CMake in Release mode with Clang 14
cmake \
  -DCMAKE_C_COMPILER=/opt/homebrew/opt/llvm@14/bin/clang-14 \
  -DCMAKE_CXX_COMPILER=/opt/homebrew/opt/llvm@14/bin/clang++ \
  -DCMAKE_BUILD_TYPE=Release \
  ..

# Build sqllogictest
echo "Building sqllogictest..."
if ! make -j$(nproc) sqllogictest; then
    echo "Failed to build sqllogictest"
    exit 1
fi

# Array of SQL test files
sql_tests=(
    "../test/sql/p3.01-seqscan.slt"
    "../test/sql/p3.02-insert.slt"
    "../test/sql/p3.03-update.slt"
    "../test/sql/p3.04-delete.slt"
    "../test/sql/p3.05-index-scan-btree.slt"
    "../test/sql/p3.06-empty-table.slt"
)

# Initialize counters and arrays for test tracking
total_tests=${#sql_tests[@]}
passed_tests=0
failed_tests=0
failed_test_names=()

echo "Starting test suite execution..."
echo "Total tests to run: $total_tests"
echo "------------------------"

# Run each SQL test
for test in "${sql_tests[@]}"; do
    printf "Running test: %s ... " "$(basename "$test")"
    if ./bin/bustub-sqllogictest "$test" --verbose > test.log 2>&1; then
        echo "[PASSED]"
        ((passed_tests++))
    else
        echo "[FAILED]"
        ((failed_tests++))
        failed_test_names+=("$test")
        echo "Error log:"
        cat test.log
    fi
done

# Clean up temporary log file
rm -f test.log

# Print summary
echo ""
echo "========================"
echo "Test Execution Summary:"
echo "------------------------"
echo "Total tests:  $total_tests"
echo "Passed:      $passed_tests"
echo "Failed:      $failed_tests"

# If there were failures, list them
if [ ${#failed_test_names[@]} -ne 0 ]; then
    echo ""
    echo "Failed tests:"
    for failed in "${failed_test_names[@]}"; do
        echo "- $(basename "$failed")"
    done
    exit 1
fi

echo ""
if [ $passed_tests -eq $total_tests ]; then
    echo "All tests passed successfully!"
else
    echo "Some tests failed. Please check the output above."
fi