#!/bin/bash

# Script to run a test continuously until it fails
# After each successful run, removes the log file so only the failed run's log remains

TEST_NAME="StateTransferWithSIFSAndEvictionTest"
LOG_FILE="core/target/${TEST_NAME}-infinispan-core.log.gz"
LOG_FILE2="core/target/infinispan-infinispan-core.log.gz"
RUN_COUNT=0
START_TIME=$(date +%s)

echo "=========================================="
echo "Running test continuously until failure"
echo "Test: ${TEST_NAME}"
echo "Started at: $(date)"
echo "=========================================="
echo

while true; do
    RUN_COUNT=$((RUN_COUNT + 1))
    RUN_START=$(date +%s)

    echo "----------------------------------------"
    echo "Run #${RUN_COUNT} started at $(date)"
    echo "----------------------------------------"

    # Run the test
    ./mvnw test -pl core -Dtest=${TEST_NAME} -PtraceTests
    EXIT_CODE=$?

    RUN_END=$(date +%s)
    RUN_DURATION=$((RUN_END - RUN_START))
    TOTAL_DURATION=$((RUN_END - START_TIME))

    if [ $EXIT_CODE -eq 0 ]; then
        echo
        echo "✓ Run #${RUN_COUNT} PASSED (took ${RUN_DURATION}s)"
        echo

        # Remove the log file to save space and keep only the failed run's log
        if [ -f "${LOG_FILE}" ]; then
            rm -f "${LOG_FILE}"
            echo "Removed log file: ${LOG_FILE}"
        fi

        if [ -f "${LOG_FILE2}" ]; then
            rm -f "${LOG_FILE2}"
            echo "Removed log file: ${LOG_FILE2}"
        fi


        echo "Continuing to next run..."
        echo
    else
        echo
        echo "=========================================="
        echo "✗ Run #${RUN_COUNT} FAILED after ${RUN_DURATION}s"
        echo "=========================================="
        echo
        echo "Test failed after ${RUN_COUNT} run(s)"
        echo "Total execution time: ${TOTAL_DURATION}s ($((TOTAL_DURATION / 60)) minutes)"
        echo "Failed at: $(date)"

        if [ -f "${LOG_FILE}" ]; then
            echo
            echo "Log file preserved at: ${LOG_FILE}"
            echo "To view: gunzip -c ${LOG_FILE} | less"
        fi

        echo
        echo "=========================================="
        exit 1
    fi
done
