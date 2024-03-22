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

#include "engine_duckdb.hpp"


// ------------------------------
// Functions

// >> DuckDB-specific function renaming and validation

namespace duckdb {

  // >> Static data and related functions for mapping functions from substrait -> duckdb

  static FunctionRenameMap engine_remapped_functions {
     {"modulus"    , "mod"      }
    ,{"std_dev"    , "stddev"   }
    ,{"starts_with", "prefix"   }
    ,{"ends_with"  , "suffix"   }
    ,{"substring"  , "substr"   }
    ,{"char_length", "length"   }
    ,{"is_nan"     , "isnan"    }
    ,{"is_finite"  , "isfinite" }
    ,{"is_infinite", "isinf"    }
    ,{"like"       , "~~"       }
    ,{"extract"    , "date_part"}
  };

  string RemoveExtension(string &function_name) {
    string name;

    for (auto &c : function_name) {
      if (c == ':') { break; }
      name += c;
    }

    return name;
  }

  string RemapFunctionName(string &function_name) {
    string name { RemoveExtension(function_name) };

    auto it = engine_remapped_functions.find(name);
    if (it != engine_remapped_functions.end()) { name = it->second; }

    return name;
  }


  // >> Static data and related functions for extraction of date subfields

  static case_insensitive_set_t engine_date_subfields {
     "year"       , "month"       , "day"
    ,"decade"     , "century"     , "millenium"
    ,"quarter"
    ,"microsecond", "milliseconds", "second"
    ,"minute"     , "hour"
  };

  void AssertValidDateSubfield(const string& subfield) {
    D_ASSERT(engine_date_subfields.count(subfield));
  }

} // namespace: duckdb


namespace duckdb {

  //! Constructor for DuckDBTranslator
  DuckDBTranslator::DuckDBTranslator(ClientContext &ctxt): context(ctxt) {
    t_conn        = make_uniq<Connection>(*ctxt.db);
    functions_map = make_uniq<mohair::SubstraitFunctionMap>();

    // create an http state, but I don't know what this is for
    auto http_state = HTTPState::TryGetState(*(t_conn->context));
    http_state->Reset();
  }

  // >> Entry points for substrait plan (json or binary) -> duckdb logical plan
  shared_ptr<DuckLogicalPlan>
  DuckDBTranslator::TranspilePlanMessage(shared_ptr<DuckSystemPlan> sys_plan) {
    shared_ptr<Relation> plan_rel = sys_plan->engine;

    // Transform Relation to QueryNode and wrap in a SQLStatement
    auto plan_wrapper  = make_uniq<SelectStatement>();
    plan_wrapper->node = plan_rel->GetQueryNode();

    // Create a planner to go from SQLStatement -> LogicalOperator
    Planner planner { context };
    planner.CreatePlan(std::move(plan_wrapper));
    shared_ptr<LogicalOperator> logical_plan { std::move(planner.plan) };

    return make_shared<DuckLogicalPlan>(sys_plan->substrait, logical_plan);
  }

  shared_ptr<DuckPhysicalPlan>
  DuckDBTranslator::TranslateLogicalPlan( shared_ptr<DuckLogicalPlan> engine_plan
                                         ,bool                        optimize) {
    // Make a copy that is a unique_ptr
    auto logical_plan = engine_plan->engine->Copy(context);

    // optimization
    if (optimize) {
      shared_ptr<Binder> binder    { Binder::CreateBinder(context) };
      Optimizer          optimizer { *binder, context };

      logical_plan = optimizer.Optimize(std::move(logical_plan));
    }

    // transformation to physical plan
    PhysicalPlanGenerator physical_planner { context };
    shared_ptr<PhysicalOperator> physical_plan {
      physical_planner.CreatePlan(std::move(logical_plan))
    };

    return make_shared<DuckPhysicalPlan>(engine_plan->substrait, physical_plan);
  }

  bool ShouldKeepExecuting(PendingExecutionResult& exec_result) {
    switch (exec_result) {
      case PendingExecutionResult::RESULT_NOT_READY:
      case PendingExecutionResult::RESULT_READY:
        break;

      case PendingExecutionResult::BLOCKED:
        std::cout << "\t[Executor]: blocked" << std::endl;
        break;

      case PendingExecutionResult::NO_TASKS_AVAILABLE:
        std::cout << "\t[Executor]: waiting for tasks" << std::endl;
        break;

      case PendingExecutionResult::EXECUTION_ERROR:
        std::cerr << "\t[Executor]: execution error" << std::endl;
        return false;

      default:
        std::cerr << "\t[Executor]: unknown execution result type" << std::endl;
        return false;
    }

    return true;
  }

  unique_ptr<QueryResult> DuckDBExecutor::Execute() {
    constexpr bool dry_run       { false };
    Executor       plan_executor { context };

    plan_executor.Initialize(
      PhysicalResultCollector::GetResultCollector(context, plan_data)
    );

    auto exec_result = plan_executor.ExecuteTask(dry_run);
    while (exec_result != PendingExecutionResult::RESULT_READY) {
      if (not ShouldKeepExecuting(exec_result)) {
        std::cerr << "\t\t" << plan_executor.GetError().Message() << std::endl;
        break;
      }

      exec_result = plan_executor.ExecuteTask(dry_run);
    }

    if (    exec_result == PendingExecutionResult::RESULT_READY
        and plan_executor.HasResultCollector()) {
      return std::move(plan_executor.GetResult());
    }

    return nullptr;
  }

} // namespace: duckdb
