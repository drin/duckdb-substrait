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
#include <iostream> // for debugging

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/enums/set_operation_type.hpp"

#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/prepared_statement_data.hpp"

#include "duckdb/main/relation/join_relation.hpp"
#include "duckdb/main/relation/cross_product_relation.hpp"
#include "duckdb/main/relation/limit_relation.hpp"
#include "duckdb/main/relation/projection_relation.hpp"
#include "duckdb/main/relation/setop_relation.hpp"
#include "duckdb/main/relation/aggregate_relation.hpp"
#include "duckdb/main/relation/filter_relation.hpp"
#include "duckdb/main/relation/order_relation.hpp"

#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/expression/list.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"

#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/table_filter.hpp"

#include "duckdb/optimizer/optimizer.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/execution/operator/helper/physical_result_collector.hpp"

#include "custom_extensions/custom_extensions.hpp"
#include "plans.hpp"


// ------------------------------
// Convenience aliases

namespace skysubstrait = skytether::substrait;
namespace skymohair    = skytether::mohair;

using duckdb::unique_ptr;
using std::string;

using FunctionRenameMap = std::unordered_map<string, string>;

using DuckSystemPlan    = mohair::SystemPlan<duckdb::Relation>;
using DuckLogicalPlan   = mohair::SystemPlan<duckdb::LogicalOperator>;
using DuckPhysicalPlan  = mohair::SystemPlan<duckdb::PhysicalOperator>;


// ------------------------------
// Convenience functions

namespace duckdb {

  //! Remove extension id from a function name
  string RemoveExtension(string &function_name);

  //! Return the duckdb function name for the given substrait function name
  string RemapFunctionName(string &function_name);

  //! Validate the subfield name is supported by duckdb date types
  void   AssertValidDateSubfield(const string& subfield);

  //! Convenience function to convert a substrait type to a logical duckdb type
  LogicalType SubstraitToDuckType(const skysubstrait::Type& s_type);

} // namespace: duckdb


// ------------------------------
// Data Classes for binding table functions

namespace duckdb {

  // Forward declaration to keep TableFunctionData types visible
  struct DuckDBTranslator;

  struct FnDataSubstraitTranslation : public TableFunctionData {
    FnDataSubstraitTranslation() = default;

    unique_ptr<DuckDBTranslator>      translator;
    shared_ptr<DuckSystemPlan>        sys_plan;
    shared_ptr<DuckLogicalPlan>       engine_plan;
    shared_ptr<DuckPhysicalPlan>      exec_plan;
    shared_ptr<PreparedStatementData> plan_data;
    bool                              enable_optimizer;
    bool                              finished { false };
  };


  struct FnDataSubstraitExecution : public TableFunctionData {
    FnDataSubstraitExecution() = default;

    unique_ptr<DuckDBTranslator> translator;
    unique_ptr<DuckSystemPlan>   sys_plan;
    unique_ptr<QueryResult>      result;
  };

} // namespace: duckdb


// ------------------------------
// Classes for engine-specific translations

namespace duckdb {


  //! Translator from substrait plan to DuckDB plans
  struct DuckDBTranslator {

    //! Initializes a Translator instance
    DuckDBTranslator(ClientContext& context);

    //! Transforms Substrait Plan to DuckDB Relation
    unique_ptr<DuckSystemPlan>   TranslatePlanMessage(const string& serialized_msg);
    unique_ptr<DuckSystemPlan>   TranslatePlanJson(const string& json_msg);

    //! Transforms DuckDB Relation to DuckDB Logical Operator
    shared_ptr<DuckLogicalPlan>  TranspilePlanMessage(shared_ptr<DuckSystemPlan> sys_plan);

    //! Transforms DuckDB Relation to DuckDB Physical Operator
    shared_ptr<DuckPhysicalPlan> TranslateLogicalPlan( shared_ptr<DuckLogicalPlan> engine_plan
                                                      ,bool                        optimize);

    private:
      ClientContext&                           context;
      unique_ptr<Connection>                   t_conn;
      unique_ptr<mohair::SubstraitFunctionMap> functions_map;

    // >> Internal functions
    private:
      // Helpers for TranslateReadOp
      shared_ptr<Relation>
      ScanNamedTable(const skysubstrait::ReadRel::NamedTable& named_table);

      shared_ptr<Relation>
      ScanFileListParquet(const skysubstrait::ReadRel::LocalFiles& local_files);

      shared_ptr<Relation>
      ScanFileListArrow(const skysubstrait::ReadRel::LocalFiles& local_files);

      shared_ptr<Relation>
      ScanFileList(const skysubstrait::ReadRel::LocalFiles& local_files);

      shared_ptr<Relation> TranslateRootOp(const skysubstrait::RelRoot& sop);
      shared_ptr<Relation> TranslateOp    (const skysubstrait::Rel&     sop);

      //! Translate a substrait expression to a duckdb expression
      unique_ptr<ParsedExpression> TranslateExpr(const skysubstrait::Expression &sexpr);

      // >> Internal translation functions for operators
      // NOTE: these member methods eventually use t_conn and functions_map
      shared_ptr<Relation> TranslateJoinOp         (const skysubstrait::JoinRel&      sjoin);
      shared_ptr<Relation> TranslateCrossProductOp (const skysubstrait::CrossRel&     scross);
      shared_ptr<Relation> TranslateFetchOp        (const skysubstrait::FetchRel&     slimit);
      shared_ptr<Relation> TranslateFilterOp       (const skysubstrait::FilterRel&    sfilter);
      shared_ptr<Relation> TranslateProjectOp      (const skysubstrait::ProjectRel&   sproj);
      shared_ptr<Relation> TranslateAggregateOp    (const skysubstrait::AggregateRel& saggr);
      shared_ptr<Relation> TranslateReadOp         (const skysubstrait::ReadRel&      sget);
      shared_ptr<Relation> TranslateSortOp         (const skysubstrait::SortRel&      ssort);
      shared_ptr<Relation> TranslateSetOp          (const skysubstrait::SetRel&       sset);

      shared_ptr<Relation> TranslateSkyRel         (const skymohair::SkyRel& sky_rel);

      //! Translate Substrait Sort Order to DuckDB Order
      OrderByNode TranslateOrder(const skysubstrait::SortField& sordf);

      // >> Internal translation functions for expressions
      unique_ptr<ParsedExpression> TranslateSelectionExpr(const skysubstrait::Expression& sexpr);
      unique_ptr<ParsedExpression> TranslateIfThenExpr   (const skysubstrait::Expression& sexpr);
      unique_ptr<ParsedExpression> TranslateCastExpr     (const skysubstrait::Expression& sexpr);
      unique_ptr<ParsedExpression> TranslateInExpr       (const skysubstrait::Expression& sexpr);

      unique_ptr<ParsedExpression>
      TranslateLiteralExpr(const skysubstrait::Expression::Literal& slit);

      unique_ptr<ParsedExpression>
      TranslateScalarFunctionExpr(const skysubstrait::Expression& sexpr);
  };

  struct DuckDBExecutor {
    ClientContext&         context;
    PreparedStatementData& plan_data;

    DuckDBExecutor(ClientContext& context, PreparedStatementData& plan_data)
      : context(context), plan_data(plan_data) {}

    unique_ptr<QueryResult> Execute();
  };

} // namespace duckdb
