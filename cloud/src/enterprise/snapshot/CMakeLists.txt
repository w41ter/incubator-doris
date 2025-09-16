# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/enterprise/snapshot/")

set(ENTERPRISE_SNAPSHOT_SOURCES snapshot_manager.cpp)

add_compile_definitions(-DFEATURE_ENTERPRISE_SNAPSHOT)
add_library(EnterpriseSnapshot STATIC ${ENTERPRISE_SNAPSHOT_SOURCES})
target_include_directories(EnterpriseSnapshot PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/)

# Export the library to parent scope
set(DORIS_CLOUD_LIBS EnterpriseSnapshot ${DORIS_CLOUD_LIBS} PARENT_SCOPE)
