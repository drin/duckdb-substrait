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

#include "duckdb/parser/parser.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/http_state.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/enums/set_operation_type.hpp"

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
    // Transpilation functions (substrait -> logical)

    //! Transpile a substrait plan to a duckdb execution plan
    unique_ptr<LogicalOperator> SystemPlanFromSubstraitPlan(substrait::Plan& plan);

    //! Transpile a substrait plan to a duckdb execution plan
    unique_ptr<LogicalOperator> TranspileRootOp(const substrait::RelRoot& sop);

    //! Transpile a substrait operator to a duckdb execution operator
    unique_ptr<LogicalOperator> TranspileOp(const substrait::Rel& sop);

    //! Transpile a substrait expression to a duckdb expression
    unique_ptr<ParsedExpression> TranspileExpr(const substrait::Expression &sexpr);

    // >> Internal transpilation functions for operators
    // NOTE: these are member methods because TranspileReadOp uses this->conn
    unique_ptr<Relation> TranspileJoinOp         (const substrait::JoinRel&      sjoin);
    unique_ptr<Relation> TranspileCrossProductOp (const substrait::CrossRel&     scross);
    unique_ptr<Relation> TranspileFetchOp        (const substrait::FetchRel&     slimit);
    unique_ptr<Relation> TranspileFilterOp       (const substrait::FilterRel&    sfilter);
    unique_ptr<Relation> TranspileProjectOp      (const substrait::ProjectRel&   sproj);
    unique_ptr<Relation> TranspileAggregateOp    (const substrait::AggregateRel& saggr);
    unique_ptr<Relation> TranspileReadOp         (const substrait::ReadRel&      sget);
    unique_ptr<Relation> TranspileSortOp         (const substrait::SortRel&      ssort);
    unique_ptr<Relation> TranspileSetOp          (const substrait::SetRel&       sset);

    //! Transpile Substrait Sort Order to DuckDB Order
    OrderByNode TranspileOrder(const substrait::SortField& sordf);


    // >> Internal transpilation functions for expressions
    unique_ptr<ParsedExpression> TranspileSelectionExpr(const substrait::Expression& sexpr);
    unique_ptr<ParsedExpression> TranspileIfThenExpr   (const substrait::Expression& sexpr);
    unique_ptr<ParsedExpression> TranspileCastExpr     (const substrait::Expression& sexpr);
    unique_ptr<ParsedExpression> TranspileInExpr       (const substrait::Expression& sexpr);

    unique_ptr<ParsedExpression>
    TranspileLiteralExpr(const substrait::Expression::Literal& slit);

    unique_ptr<ParsedExpression>
    TranspileScalarFunctionExpr(const substrait::Expression& sexpr);


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
    shared_ptr<LogicalOperator> TranspilePlanMessage(const string& serialized_msg);

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
