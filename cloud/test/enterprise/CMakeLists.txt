# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/test/")

# where to put generated libraries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/test/")

set(TEST_LINK_LIBS ResourceManager ${TEST_LINK_LIBS})

message(STATUS "Enable enterprise edition tests")

# ATTN: adding prefix "enterprise_" to avoid name conflict with the open source tests
add_executable(enterprise_meta_service_snapshot_test meta_service_snapshot_test.cpp)
add_executable(enterprise_meta_service_http_test meta_service_http_test.cpp)
target_link_libraries(enterprise_meta_service_snapshot_test ${TEST_LINK_LIBS})
target_link_libraries(enterprise_meta_service_http_test ${TEST_LINK_LIBS})
