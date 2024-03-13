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

// >> Utility functions

namespace duckdb {

  //! Set static lookup table for substrait -> duckdb function names
  const std::unordered_map<std::string, std::string>
  DuckDBEnginePlan::function_names_remap = {
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

  //! Set static reference table for duckdb date subfields
  const case_insensitive_set_t
  DuckDBEnginePlan::valid_extract_subfields = {
     "year"
    ,"month"
    ,"day"
    ,"decade"
    ,"century"
    ,"millenium"
    ,"quarter"
    ,"microsecond"
    ,"milliseconds"
    ,"second"
    ,"minute"
    , "hour"
  };


  // >> DuckDBEnginePlan
  DuckDBEnginePlan::DuckDBEnginePlan(Connection& conn): conn(conn) {}

  void DuckDBEnginePlan::RegisterExtensionFunctions(substrait::Plan& plan) {
    for (auto &sext : plan.extensions()) {
      if (!sext.has_extension_function()) { continue; }

      const auto fn_anchor     = sext.extension_function().function_anchor();
      functions_map[fn_anchor] = sext.extension_function().name();
    }
  }

  void DuckDBEnginePlan::VerifyCorrectExtractSubfield(const string& subfield) {
    D_ASSERT(DuckDBEnginePlan::valid_extract_subfields.count(subfield));
  }

  string DuckDBEnginePlan::RemoveExtension(string &function_name) {
    // Lets first drop any extension id
    string name;
    for (auto &c : function_name) {
      if (c == ':') { break; }
      name += c;
    }

    return name;
  }

  string DuckDBEnginePlan::RemapFunctionName(string &function_name) {
    string name { RemoveExtension(function_name) };

    auto it = function_names_remap.find(name);
    if (it != function_names_remap.end()) { name = it->second; }

    return name;
  }

  string DuckDBEnginePlan::FindFunction(uint64_t id) {
    if (functions_map.find(id) == functions_map.end()) {
      throw InternalException("Could not find aggregate function " + to_string(id));
    }

    return functions_map[id];
  }

  shared_ptr<Relation>
  DuckDBEnginePlan::EnginePlanFromSubstraitPlan(substrait::Plan& plan) {
    if (plan.relations().empty()) {
      throw InvalidInputException("Substrait Plan does not have a SELECT statement");
    }

    RegisterExtensionFunctions(plan);

    return TranslateRootOp(plan.relations(0).root());
  }

} // namespace duckdb
