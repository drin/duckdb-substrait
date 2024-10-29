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

#include "plans.hpp"


// ------------------------------
// Macros and Type Aliases

namespace skysubstrait = skytether::substrait;


// ------------------------------
// Classes and structs

namespace mohair {

  // ------------------------------
  // Methods for SubstraitFunctionMap

  //! Register extension functions from the substrait plan in the function map
  void SubstraitFunctionMap::RegisterExtensionFunctions(skysubstrait::Plan& plan) {
    for (auto &sext : plan.extensions()) {
      if (!sext.has_extension_function()) { continue; }

      const auto fn_anchor = sext.extension_function().function_anchor();
      fn_map[fn_anchor]    = sext.extension_function().name();
    }
  }

  string SubstraitFunctionMap::FindExtensionFunction(uint64_t id) {
    if (fn_map.find(id) == fn_map.end()) {
      throw duckdb::InternalException(
        "Could not find aggregate function " + std::to_string(id)
      );
    }

    return fn_map[id];
  }


  // ------------------------------
  // Builder Functions for SystemPlan

  //! Builder function that constructs SystemPlan from a serialized substrait message
  unique_ptr<skysubstrait::Plan>
  SubstraitPlanFromSubstraitMessage(const string& serialized_msg) {
    auto plan = duckdb::make_uniq<skysubstrait::Plan>();
    if (not plan->ParseFromString(serialized_msg)) {
      throw std::runtime_error("Error parsing serialized Substrait Plan");
    }

    return plan;
  }

  //! Builder function that constructs SystemPlan from a JSON-formatted substrait message
  unique_ptr<skysubstrait::Plan>
  SubstraitPlanFromSubstraitJson(const string& json_msg) {
    auto plan = duckdb::make_uniq<skysubstrait::Plan>();

    ProtoStatus status = JsonStringToMessage(json_msg, plan.get());
    if (not status.ok()) {
      throw std::runtime_error("Error parsing JSON Substrait Plan: " + status.ToString());
    }

    return plan;
  }

} // namespace: mohair
