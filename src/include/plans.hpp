// ------------------------------
// License
//
// Copyright 2024 Aldrin Montana
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


// ------------------------------
// Dependencies
#pragma once

#include <string>
#include <unordered_map>

#include "google/protobuf/util/json_util.h"

#include "duckdb.hpp"

#include "mohair-substrait/substrait/plan.pb.h"
#include "mohair-substrait/substrait/algebra.pb.h"


// ------------------------------
// Macros and Type Aliases

// Standard types
using std::string;
using std::unordered_map;

// types and functions from duckdb namespace
using duckdb::unique_ptr;
using duckdb::shared_ptr;

// types and functions from protobuf
using ProtoStatus = absl::Status;
using google::protobuf::util::JsonStringToMessage;


// ------------------------------
// Classes and structs

namespace mohair {

  //! Alias template for transpilation functions
  template <typename LogicalOpType>
  using TranspileSysPlanFnType = std::function<substrait::Plan(LogicalOpType&)>;

  template <typename LogicalOpType>
  using TranspileSubPlanFnType = std::function<LogicalOpType(substrait::Plan&)>;

  //! Templated class to hold a substrait plan and a "system plan".
  /** The system plan is flexibly represented by the templated type so that any particular
   *  system can translate to the preferred level of abstraction.
   */
  template <typename LogicalOpType>
  struct SystemPlan {
    //! A system-level query plan represented as substrait
    shared_ptr<substrait::Plan> substrait;

    //! A system-level query plan represented by a specific query engine
    shared_ptr<LogicalOpType>   engine;


    SystemPlan( shared_ptr<substrait::Plan> s_plan
               ,shared_ptr<LogicalOpType>   e_plan)
      :  substrait(s_plan), engine(e_plan) {}


    // TODO:
    // arrow::Table Execute();
  };

  //! Builder function that constructs SystemPlan from a serialized substrait message
  unique_ptr<substrait::Plan> SubstraitPlanFromSubstraitMessage(const string& serialized_msg);

  //! Builder function that constructs SystemPlan from a JSON-formatted substrait message
  unique_ptr<substrait::Plan> SubstraitPlanFromSubstraitJson(const string& json_msg);

} // namespace: mohair


// >> Code for managing substrait function extensions. Likely to move in the future.
namespace mohair {

  struct SubstraitFunctionMap {
    //! Registry of substrait function extensions used in substrait plan
    unordered_map<uint64_t, string> fn_map;

    void   RegisterExtensionFunctions(substrait::Plan& plan);
    string FindExtensionFunction(uint64_t id);
  };

} // namespace: mohair
