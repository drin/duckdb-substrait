# ------------------------------
# Project Configuration
cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME substrait)
set(EXTENSION_NAME ${TARGET_NAME}_extension)

project(${TARGET_NAME})

# Options (defaults should be compatible with existing builds)
option(SUBSTRAIT_SRCDIR "Directory containing substrait-protocol sources"
  third_party/substrait)

option(MOHAIR_SRCDIR "Directory containing mohair-protocol sources")


# ------------------------------
# Dependencies

# >> External dependencies

# Protobuf
find_package(Protobuf REQUIRED)

# conditionally, depend on absl
if (Protobuf_VERSION VERSION_GREATER_EQUAL 4)
  find_package(absl REQUIRED)
endif()


# ------------------------------
# Source files

# >> Headers
include_directories(src/include)
include_directories(SUBSTRAIT_SRCDIR)

if(MOHAIR_SRCDIR)
  include_directories(MOHAIR_SRCDIR)
endif()

# >> Sources
set(SUBSTRAIT_PROTO_SOURCES
  ${SUBSTRAIT_SRCDIR}/substrait/algebra.pb.cc
  ${SUBSTRAIT_SRCDIR}/substrait/capabilities.pb.cc
  ${SUBSTRAIT_SRCDIR}/substrait/function.pb.cc
  ${SUBSTRAIT_SRCDIR}/substrait/parameterized_types.pb.cc
  ${SUBSTRAIT_SRCDIR}/substrait/plan.pb.cc
  ${SUBSTRAIT_SRCDIR}/substrait/type.pb.cc
  ${SUBSTRAIT_SRCDIR}/substrait/type_expressions.pb.cc
  ${SUBSTRAIT_SRCDIR}/substrait/extensions/extensions.pb.cc)

if(MOHAIR_SRCDIR)
  list(APPEND SUBSTRAIT_PROTO_SOURCES
    ${MOHAIR_SRCDIR}/mohair/algebra.pb.cc
    ${MOHAIR_SRCDIR}/mohair/topology.pb.cc)
endif()

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

# >> generated protobuf code and protobuf dep
add_library(LIB_SUBSTRAIT STATIC ${SUBSTRAIT_PROTO_SOURCES})

# configure lib
set_target_properties(LIB_SUBSTRAIT PROPERTIES POSITION_INDEPENDENT_CODE ON)

# include dirs for lib
target_include_directories(LIB_SUBSTRAIT PUBLIC ${SUBSTRAIT_SRCDIR})
if(MOHAIR_SRCDIR)
  target_include_directories(LIB_SUBSTRAIT PUBLIC ${MOHAIR_SRCDIR})
endif()

# dependencies for lib
target_link_libraries(LIB_SUBSTRAIT PUBLIC protobuf::libprotobuf)
if (Protobuf_VERSION VERSION_GREATER_EQUAL 4)
  # this is needed for generated protobuf libs that use ABSL_DCHECK*()
  target_link_libraries(LIB_SUBSTRAIT PUBLIC absl::status
                                             absl::log_internal_check_op)
endif()


# >> loadable duckdb extension
add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})
target_link_libraries(${EXTENSION_NAME} PUBLIC LIB_SUBSTRAIT)

# configure the loadable extension
set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})


# ------------------------------
# Install commands

# >> substrait extension
install(
  TARGETS ${EXTENSION_NAME} LIB_SUBSTRAIT
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

