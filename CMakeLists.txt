# ------------------------------
# Project Configuration
cmake_minimum_required(VERSION 3.10...3.29)

# Set extension name here
set(EXT_BASENAME substrait)
set(EXTENSION_NAME ${EXT_BASENAME}_extension)

project(${EXT_BASENAME})


# ------------------------------
# Locate Dependencies

# >> Third-party dependencies

# Protobuf
set(Protobuf_USE_STATIC_LIBS ON)
find_package(Protobuf REQUIRED)

# conditionally, depend on absl
if (Protobuf_VERSION VERSION_GREATER_EQUAL 4)
  find_package(absl       REQUIRED)
  find_package(utf8_range REQUIRED)
endif()


# >> Prebuilt (managed by us but built independently)
# TODO: figure out how to push some logic into a FindPackage or Config file

# NOTE: this seems stupid but I haven't found anything to make this simpler
set(suffix_sharedlib ".so")
if(APPLE)
  set(suffix_sharedlib ".dylib")
endif()

# |> library for mohair

# check for shared library first
find_library(Mohair_LIBRARIES "libmohair${suffix_sharedlib}")

if(${Mohair_LIBRARIES} STREQUAL "Mohair_LIBRARIES-NOTFOUND")
  message(FATAL_ERROR "Could not find mohair")

else()
  message(STATUS "Found mohair: " ${Mohair_LIBRARIES})

endif()

# headers for mohair
find_path(Mohair_INCLUDE_DIRS skytether REQUIRED)

if(${Mohair_INCLUDE_DIRS} STREQUAL "Mohair_INCLUDE_DIRS-NOTFOUND")
  message(FATAL_ERROR "Could not find mohair include dir")

else()
  message(STATUS "Found mohair headers: " ${Mohair_INCLUDE_DIRS})

endif()

# We set these to look like what we want a FindPackage to eventually publish
set(Mohair::libmohair ${Mohair_LIBRARIES})

# NOTE: this breaks our hack for `build_loadable_extension`
# list(APPEND Mohair_INCLUDE_DIRS
#   ${Mohair_INCLUDE_DIRS}/mohair)


# ------------------------------
# Source files

# >> Headers
include_directories(src/include)

# custom sources for mohair integration
set(MOHAIR_SOURCES
  src/plans.cpp
  src/engine_duckdb.cpp
  src/translation/duckdb_expressions.cpp
  src/translation/duckdb_operators.cpp)
  #src/transpilation/duckdb_expressions.cpp
  #src/transpilation/duckdb_operators.cpp)

# official sources for substrait integration
set(SUBSTRAIT_EXT_SOURCES
  src/from_substrait.cpp
  src/to_substrait.cpp)

# primary sources first, then others
set(EXTENSION_SOURCES
  src/substrait_extension.cpp
  src/custom_extensions.cpp
  src/custom_extensions_generated.cpp
  ${MOHAIR_SOURCES}
  ${SUBSTRAIT_EXT_SOURCES})


# ------------------------------
# Targets

# >> loadable duckdb extension
add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})

target_include_directories(${EXTENSION_NAME}
                           PRIVATE ${Mohair_INCLUDE_DIRS}
                                   ${Mohair_INCLUDE_DIRS}/skytether)

target_link_libraries(${EXTENSION_NAME}
                      PRIVATE ${Mohair_LIBRARIES}
                              ${Protobuf_LIBRARIES})

if (Protobuf_VERSION VERSION_GREATER_EQUAL 4)
  target_link_libraries(${EXTENSION_NAME} PRIVATE absl::flat_hash_map
                                                  absl::status
                                                  absl::statusor
                                                  absl::log_internal_check_op
                                                  absl::log_internal_conditions
                                                  utf8_range::utf8_validity)
endif()

# configure the loadable extension
set(PARAMETERS "-warnings")

build_loadable_extension(${EXT_BASENAME}
  ${PARAMETERS}
  ${EXTENSION_SOURCES}
  "-I${Mohair_INCLUDE_DIRS}"
  "-I${Mohair_INCLUDE_DIRS}/skytether")


# ------------------------------
# Targets in sub-directories

IF (DEFINED ENV{SKIP_SUBSTRAIT_C_TESTS})
  message(STATUS "Skipping substrait c tests")
ELSE()
  # NOTE: test/c/CMakeLists.txt assumes there is a /duckdb directory
  # add_subdirectory("test/c")
ENDIF()


# ------------------------------
# Install commands

# >> substrait extension
install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

