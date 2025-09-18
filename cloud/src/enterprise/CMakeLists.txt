add_definitions(-DENTERPRISE_EDITION)
message(STATUS "Enable enterprise edition features")

add_subdirectory(snapshot)

# Export the library to parent scope
set(DORIS_CLOUD_LIBS ${DORIS_CLOUD_LIBS} PARENT_SCOPE)
set(DORIS_ADDITIONAL_DEFINITIONS ${DORIS_ADDITIONAL_DEFINITIONS} PARENT_SCOPE)
