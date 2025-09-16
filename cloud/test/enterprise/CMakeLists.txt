# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/test/enterprise")

# where to put generated libraries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/test/enterprise")

set(TEST_LINK_LIBS ResourceManager ${TEST_LINK_LIBS})

message(STATUS "Enable enterprise edition tests")
