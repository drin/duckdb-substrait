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
#include "google/protobuf/text_format.h"

#include "duckdb.hpp"

#include "skyproto/substrait/plan.pb.h"
#include "skyproto/substrait/algebra.pb.h"
#include "skyproto/mohair/algebra.pb.h"


// ------------------------------
// Macros and Type Aliases

namespace mohair {

  // Namespace aliases
  namespace skysubstrait = skyproto::substrait;
  namespace skymohair    = skyproto::mohair;

  // Standard types
  using std::string;
  using std::unordered_map;

  // types and functions from duckdb namespace
  template <typename ptype>
  using duck_uptr = duckdb::unique_ptr<ptype>;

  template <typename ptype>
  using duck_sptr = duckdb::shared_ptr<ptype>;

  // types and functions from protobuf
  using ProtoStatus = absl::Status;
  using google::protobuf::util::JsonStringToMessage;

} // namespace: mohair


// ------------------------------
// Classes and structs

namespace mohair {

  //! Code for managing substrait function extensions. Likely to move in the future.
  struct SubstraitFunctionMap {
    //! Registry of substrait function extensions used in substrait plan
    unordered_map<uint64_t, string> fn_map;

    void   RegisterExtensionFunctions(skysubstrait::Plan& plan);
    string FindExtensionFunction(uint64_t id);
  };

  //! A structure holding a substrait plan and an appropriate representation for DuckDB.
  struct DuckSystemPlan {
    //! A system-level query plan represented as substrait
    duck_sptr<skysubstrait::Plan> substrait;

    //! The DuckDB representation of the substrait plan
    duck_sptr<duckdb::Relation> duck_plan;

    DuckSystemPlan( duck_sptr<skysubstrait::Plan> s_plan
                   ,duck_sptr<duckdb::Relation>   e_plan)
      : substrait(s_plan), duck_plan(e_plan) {}
  };

  //! Builder function that constructs DuckSystemPlan from a serialized substrait message
  duck_uptr<skysubstrait::Plan>
  SubstraitPlanFromSubstraitMessage(const string& serialized_msg);

  //! Builder function that constructs DuckSystemPlan from a JSON-formatted substrait message
  duck_uptr<skysubstrait::Plan>
  SubstraitPlanFromSubstraitJson(const string& json_msg);

} // namespace: mohair
