# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/enterprise/snapshot/")

set(ENTERPRISE_SNAPSHOT_SOURCES
    recycle_snapshot.cpp
    check_snapshot.cpp
    snapshot_data_migrator.cpp
    snapshot_chain_compactor.cpp
    snapshot_manager.cpp
    meta_check_snapshot.cpp)

if(BUILD_CHECK_META STREQUAL "OFF")
    list(REMOVE_ITEM ENTERPRISE_SNAPSHOT_SOURCES meta_check_snapshot.cpp)
endif ()

add_compile_definitions(FEATURE_ENTERPRISE_SNAPSHOT)
add_library(EnterpriseSnapshot STATIC ${ENTERPRISE_SNAPSHOT_SOURCES})

# Export the library to parent scope
set(DORIS_CLOUD_LIBS EnterpriseSnapshot ${DORIS_CLOUD_LIBS} PARENT_SCOPE)
set(DORIS_ADDITIONAL_DEFINITIONS -DFEATURE_ENTERPRISE_SNAPSHOT ${DORIS_ADDITIONAL_DEFINITIONS} PARENT_SCOPE)
