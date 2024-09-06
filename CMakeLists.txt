# ------------------------------
# Project attributes
cmake_minimum_required(VERSION 3.5)

# Variables for duckdb extension
set(DUCK_EXT_BASENAME substrait)
set(EXTENSION_NAME ${DUCK_EXT_BASENAME}_extension)

set(CMAKE_CXX_STANDARD 17)

# Project definition
project(${DUCK_EXT_BASENAME})


# ------------------------------
# Locate Dependencies

# >> Third-party

# Protobuf
find_package(Protobuf REQUIRED)

# conditionally, depend on absl
if (Protobuf_VERSION VERSION_GREATER_EQUAL 4)
  find_package(absl REQUIRED)
endif()


# >> Prebuilt (managed by us but built independently)

# TODO: move this to a package finder
# library for mohair-substrait
find_library(MohairSubstrait_LIBRARIES libmohair-substrait.a REQUIRED)
if(MohairSubstrait_LIBRARIES-NOTFOUND)
  message(FATAL_ERROR "Could not find mohair-substrait")

else()
  message(STATUS "Found mohair-substrait libraries: " ${MohairSubstrait_LIBRARIES})

endif()

# headers for mohair-substrait
find_path(MohairSubstrait_INCLUDE_DIRS mohair-substrait REQUIRED)
if(MohairSubstrait_INCLUDE_DIRS-NOTFOUND)
  message(FATAL_ERROR "Could not find mohair-substrait include dir")

else()
  message(STATUS "Found mohair-substrait: headers" ${MohairSubstrait_INCLUDE_DIRS})

endif()


# ------------------------------
# Source files

# >> integration between duckdb and substrait
# files for official duckdb-substrait extension
set(SUBSTRAIT_EXT_SOURCES
    src/from_substrait.cpp
    src/to_substrait.cpp)

# files for custom mohair integration
set(MOHAIR_SOURCES
    src/plans.cpp
    src/engine_duckdb.cpp
    src/translation/duckdb_expressions.cpp
    src/translation/duckdb_operators.cpp)
    #src/transpilation/duckdb_expressions.cpp
    #src/transpilation/duckdb_operators.cpp)


# >> Consolidated targets (primary sources first, then others)
set(EXTENSION_SOURCES
    src/substrait_extension.cpp
    src/custom_extensions.cpp
    src/custom_extensions_generated.cpp
    ${SUBSTRAIT_EXT_SOURCES}
    ${MOHAIR_SOURCES})


# ------------------------------
# Library targets

# >> The library for the duckdb-substrait extension itself
add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})
# set_target_properties(${EXTENSION_NAME} PROPERTIES POSITION_INDEPENDENT_CODE ON)


# ------------------------------
# Library dependencies and properties

# >> duckdb-substrait extension

# link the protobuf library (the framework itself)
target_link_libraries(${EXTENSION_NAME} PUBLIC protobuf::libprotobuf)

# conditionally, link against absl
if (Protobuf_VERSION VERSION_GREATER_EQUAL 4)
  # this is needed for generated protobuf libs that use ABSL_DCHECK*()
  target_link_libraries(${EXTENSION_NAME}
                        PUBLIC absl::status
                               absl::log_internal_check_op)
endif()

# link against generated protobuf lib (mohair-substrait)
if(MohairSubstrait_LIBRARIES)
  message(STATUS "linking mohair-substrait library: ${MohairSubstrait_LIBRARIES}")
  target_link_libraries(${EXTENSION_NAME} PUBLIC ${MohairSubstrait_LIBRARIES})
endif()


# ------------------------------
# Library headers

# >> Internal headers (in-project) common across library targets
include_directories(src/include)

# >> External headers specific to duckdb-substrait dependencies

# protobuf
target_include_directories(${EXTENSION_NAME} PUBLIC ${Protobuf_INCLUDE_DIRS})

# mohair-substrait
target_include_directories(${EXTENSION_NAME} PUBLIC ${MohairSubstrait_INCLUDE_DIRS})


# ------------------------------
# Build artifacts

set(PARAMETERS "-warnings")
message(STATUS "building loadable extension")
build_loadable_extension(
  ${DUCK_EXT_BASENAME}
  ${PARAMETERS} ${EXTENSION_SOURCES} "-I${MohairSubstrait_INCLUDE_DIRS}"
)
message(STATUS "loadable extension built")

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

