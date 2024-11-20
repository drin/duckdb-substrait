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

# |> library for mohair-substrait

# check for shared library first
find_library(MohairSubstrait_LIBRARIES "libmohair-substrait${suffix_sharedlib}")

if(${MohairSubstrait_LIBRARIES} STREQUAL "MohairSubstrait_LIBRARIES-NOTFOUND")

  # otherwise, check for static library
  message(STATUS "Searching for mohair-substrait static libraries")
  find_library(MohairSubstrait_LIBRARIES libmohair-substrait.a REQUIRED)

  if(${MohairSubstrait_LIBRARIES} STREQUAL "MohairSubstrait_LIBRARIES-NOTFOUND")
    message(FATAL_ERROR "Could not find mohair-substrait")

  else()
    message(STATUS "Found mohair-substrait static libraries: " ${MohairSubstrait_LIBRARIES})

  endif()

else()
  message(STATUS "Found mohair-substrait shared libraries: " ${MohairSubstrait_LIBRARIES})

endif()

# headers for mohair-substrait
find_path(MohairSubstrait_INCLUDE_DIRS mohair-substrait REQUIRED)

if(${MohairSubstrait_INCLUDE_DIRS} STREQUAL "MohairSubstrait_INCLUDE_DIRS-NOTFOUND")
  message(FATAL_ERROR "Could not find mohair-substrait include dir")

else()
  message(STATUS "Found mohair-substrait headers: " ${MohairSubstrait_INCLUDE_DIRS})

endif()

# We set these to look like what we want a FindPackage to eventually publish
set(MohairSubstrait::libmohair-substrait ${MohairSubstrait_LIBRARIES})

# NOTE: this breaks our hack for `build_loadable_extension`
# list(APPEND MohairSubstrait_INCLUDE_DIRS
#   ${MohairSubstrait_INCLUDE_DIRS}/mohair-substrait)


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
                           PRIVATE ${MohairSubstrait_INCLUDE_DIRS}
                                   ${MohairSubstrait_INCLUDE_DIRS}/mohair-substrait)

target_link_libraries(${EXTENSION_NAME}
                      PRIVATE ${MohairSubstrait_LIBRARIES}
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
  "-I${MohairSubstrait_INCLUDE_DIRS}"
  "-I${MohairSubstrait_INCLUDE_DIRS}/mohair-substrait")


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

