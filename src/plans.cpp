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

#include "google/protobuf/util/json_util.h"


// ------------------------------
// Macros and Type Aliases


// Functions
using duckdb::google::protobuf::util::JsonStringToMessage;


// ------------------------------
// Structs and Classes

namespace duckdb {

  //! Constructor for DuckDBTranslator
  DuckDBTranslator::DuckDBTranslator(ClientContext &ctxt): context(ctxt) {
    // initialize a new connection for the translator to use
    t_conn = make_uniq<Connection>(*ctxt.db);

    // create an http state, but I don't know what this is for
    auto http_state = HTTPState::TryGetState(*(t_conn->context));
    http_state->Reset();
  }

  // >> Entry points for substrait plan (json or binary) -> duckdb logical plan
  unique_ptr<LogicalOperator>
  DuckDBTranslator::TranspilePlanRel(shared_ptr<Relation> plan_rel) {
    // Transform Relation to QueryNode and wrap in a SQLStatement
    auto plan_wrapper  = make_uniq<SelectStatement>();
    plan_wrapper->node = plan_rel->GetQueryNode();

    // Create a planner to go from SQLStatement -> LogicalOperator
    Planner planner { context };
    planner.CreatePlan(std::move(plan_wrapper));
    return std::move(planner.plan);
  }

  // >> Entry points for substrait plan (json or binary) -> duckdb execution plan
  shared_ptr<Relation>
  DuckDBTranslator::TranslatePlanMessage(const string &serialized_msg) {
    if (not plan.ParseFromString(serialized_msg)) {
      throw std::runtime_error("Error parsing serialized Substrait Plan");
    }

    DuckDBEnginePlan engine_plan { *t_conn };
    return engine_plan.EnginePlanFromSubstraitPlan(plan);
  }

  shared_ptr<Relation>
  DuckDBTranslator::TranslatePlanJson(const string &json_msg) {
    google::protobuf::util::Status status = JsonStringToMessage(json_msg, &plan);

    if (not status.ok()) {
      throw std::runtime_error("Error parsing JSON Substrait Plan: " + status.ToString());
    }

    DuckDBEnginePlan engine_plan { *t_conn };
    return engine_plan.EnginePlanFromSubstraitPlan(plan);
  }

} // namespace: duckdb
