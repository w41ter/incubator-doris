# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/enterprise/snapshot/")

set(ENTERPRISE_SNAPSHOT_SOURCES
    recycle_snapshot.cpp
    snapshot_data_migrator.cpp
    snapshot_manager.cpp)

if (BUILD_CHECK_META STREQUAL "ON")
    set(ENTERPRISE_SNAPSHOT_SOURCES ${ENTERPRISE_SNAPSHOT_SOURCES} check_snapshot.cpp)
endif()

add_compile_definitions(-DFEATURE_ENTERPRISE_SNAPSHOT)
add_library(EnterpriseSnapshot STATIC ${ENTERPRISE_SNAPSHOT_SOURCES})
if (BUILD_CHECK_META STREQUAL "ON")
    target_compile_definitions(EnterpriseSnapshot -DBUILD_CHECK_META)
endif()

# Export the library to parent scope
set(DORIS_CLOUD_LIBS EnterpriseSnapshot ${DORIS_CLOUD_LIBS} PARENT_SCOPE)
set(DORIS_ADDITIONAL_DEFINITIONS -DFEATURE_ENTERPRISE_SNAPSHOT ${DORIS_ADDITIONAL_DEFINITIONS} PARENT_SCOPE)
