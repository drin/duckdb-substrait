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

#include <memory>
#include <string>
#include <unordered_map>
#include <iostream> // for debugging

#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/prepared_statement_data.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/http_state.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/enums/set_operation_type.hpp"

#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/execution/operator/helper/physical_result_collector.hpp"

#include "substrait/plan.pb.h"
#include "substrait/algebra.pb.h"


// ------------------------------
// Macros and Type Aliases

// Standard types
using std::string;
using std::unordered_map;

// Convenience aliases
using FunctionMap = unordered_map<uint64_t, string>;


namespace duckdb {

  //! Execution plan (physical plan) for DuckDB
  struct DuckDBEnginePlan {
    // >> Constructors

    //! Initializes a Translator instance
    DuckDBEnginePlan(Connection& conn);


    // >> Convenience functions

    //! Reusable function that updates function extensions given a substrait plan
    void RegisterExtensionFunctions(substrait::Plan& plan);

    //! Check that the subfield is supported by duckdb
    void VerifyCorrectExtractSubfield(const string& subfield);

    //! Normalize function name by dropping extension ID
    string RemoveExtension(string& function_name);

    //! Remap substrait function name to duckdb function name
    string RemapFunctionName(string& function_name);

    //! Looks up for aggregation function in functions_map
    string FindFunction(uint64_t id);


    // ------------------------------
    // Translation functions (substrait -> physical)

    //! Translate a substrait plan to a duckdb execution plan
    shared_ptr<Relation> EnginePlanFromSubstraitPlan(substrait::Plan& plan);

    //! Translate a substrait plan to a duckdb execution plan
    shared_ptr<Relation> TranslateRootOp(const substrait::RelRoot& sop);

    //! Translate a substrait operator to a duckdb execution operator
    shared_ptr<Relation> TranslateOp(const substrait::Rel& sop);

    //! Translate a substrait expression to a duckdb expression
    unique_ptr<ParsedExpression> TranslateExpr(const substrait::Expression &sexpr);

    // >> Internal translation functions for operators
    // NOTE: these are member methods because TranslateReadOp uses this->conn
    shared_ptr<Relation> TranslateJoinOp         (const substrait::JoinRel&      sjoin);
    shared_ptr<Relation> TranslateCrossProductOp (const substrait::CrossRel&     scross);
    shared_ptr<Relation> TranslateFetchOp        (const substrait::FetchRel&     slimit);
    shared_ptr<Relation> TranslateFilterOp       (const substrait::FilterRel&    sfilter);
    shared_ptr<Relation> TranslateProjectOp      (const substrait::ProjectRel&   sproj);
    shared_ptr<Relation> TranslateAggregateOp    (const substrait::AggregateRel& saggr);
    shared_ptr<Relation> TranslateReadOp         (const substrait::ReadRel&      sget);
    shared_ptr<Relation> TranslateSortOp         (const substrait::SortRel&      ssort);
    shared_ptr<Relation> TranslateSetOp          (const substrait::SetRel&       sset);

    //! Translate Substrait Sort Order to DuckDB Order
    OrderByNode TranslateOrder(const substrait::SortField& sordf);


    // >> Internal translation functions for expressions
    unique_ptr<ParsedExpression> TranslateSelectionExpr(const substrait::Expression& sexpr);
    unique_ptr<ParsedExpression> TranslateIfThenExpr   (const substrait::Expression& sexpr);
    unique_ptr<ParsedExpression> TranslateCastExpr     (const substrait::Expression& sexpr);
    unique_ptr<ParsedExpression> TranslateInExpr       (const substrait::Expression& sexpr);

    unique_ptr<ParsedExpression>
    TranslateLiteralExpr(const substrait::Expression::Literal& slit);

    unique_ptr<ParsedExpression>
    TranslateScalarFunctionExpr(const substrait::Expression& sexpr);


    // >> Member variables
    private:
      //! DuckDB `Connection`
      Connection&  conn;

      //! DuckDB Binder, used for generating indices
      shared_ptr<Binder> binder;

      //! Map of registered substrait function extensions
      FunctionMap functions_map;

      //! Remap substrait function names to DuckDB function names
      static const unordered_map<string, string> function_names_remap;
      static const case_insensitive_set_t        valid_extract_subfields;
  };

  //! Translator from substrait plan to DuckDB plans
  struct DuckDBTranslator {
    //! Initializes a Translator instance
    DuckDBTranslator(ClientContext& context);

    //! Transforms serialized Substrait Plan to DuckDB Relation
    unique_ptr<LogicalOperator> TranspilePlanRel(shared_ptr<Relation> plan_rel);

    //! Transforms serialized Substrait Plan to DuckDB Relation
    shared_ptr<Relation> TranslatePlanMessage(const string& serialized_msg);

    //! Transforms json-formatted Substrait Plan to DuckDB Relation
    shared_ptr<Relation> TranslatePlanJson(const string& json_msg);

    private:
      //! DuckDB `ClientContext` inherited from initial DuckDB Connection
      ClientContext &context;

      //! DuckDB `Connection` initiated by `DuckDBTranslator`
      unique_ptr<Connection> t_conn;

      //! Substrait Plan
      substrait::Plan plan;
  };

} // namespace duckdb
