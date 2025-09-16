add_definitions(-DENTERPRISE_EDITION)
message(STATUS "Enable enterprise edition features")

add_subdirectory(snapshot)

# Export the library to parent scope
set(DORIS_CLOUD_LIBS ${DORIS_CLOUD_LIBS} PARENT_SCOPE)
