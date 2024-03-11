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
// Macros and Type Aliases


// ------------------------------
// Functions

namespace duckdb {

  static duckdb::SetOperationType
  TranspileSetOperationType(substrait::SetRel::SetOp setop) {
    switch (setop) {
      case substrait::SetRel::SET_OP_UNION_ALL: {
        return duckdb::SetOperationType::UNION;
      }

      case substrait::SetRel::SET_OP_MINUS_PRIMARY: {
        return duckdb::SetOperationType::EXCEPT;
      }

      case substrait::SetRel::SET_OP_INTERSECTION_PRIMARY: {
        return duckdb::SetOperationType::INTERSECT;
      }

      default: {
        throw duckdb::NotImplementedException(
           "SetOperationType transform not implemented for SetRel type %d"
          ,setop
        );
      }
    }
  }


  // TODO select the right join operator type here
  // TODO: this is where I'm stopping to test out `Relation` -> `QueryNode` ->
  // `LogicalOperator`
  static duckdb::LogicalJoin
  TranspileJoinOp(const substrait::JoinRel& sjoin) {
    switch (sjoin.type()) {
      case substrait::JoinRel::JOIN_TYPE_INNER:
        return make_uniq<LogicalComparisonJoin>(
           duckdb::JoinType::INNER
          ,duckdb::LogicalOperatorType::LOGICAL_COMPARISON_JOIN
        );

      case substrait::JoinRel::JOIN_TYPE_LEFT:   return duckdb::JoinType::LEFT;
      case substrait::JoinRel::JOIN_TYPE_RIGHT:  return duckdb::JoinType::RIGHT;
      case substrait::JoinRel::JOIN_TYPE_SINGLE: return duckdb::JoinType::SINGLE;
      case substrait::JoinRel::JOIN_TYPE_SEMI:   return duckdb::JoinType::SEMI;

      default:
        throw InternalException("Unsupported join type");
    }
  }


  OrderByNode
  DuckDBEnginePlan::TranspileOrder(const substrait::SortField& sordf) {
    OrderType       dordertype;
    OrderByNullType dnullorder;

    switch (sordf.direction()) {
      case substrait::SortField::SORT_DIRECTION_ASC_NULLS_FIRST:
        dordertype = OrderType::ASCENDING;
        dnullorder = OrderByNullType::NULLS_FIRST;
        break;

      case substrait::SortField::SORT_DIRECTION_ASC_NULLS_LAST:
        dordertype = OrderType::ASCENDING;
        dnullorder = OrderByNullType::NULLS_LAST;
        break;

      case substrait::SortField::SORT_DIRECTION_DESC_NULLS_FIRST:
        dordertype = OrderType::DESCENDING;
        dnullorder = OrderByNullType::NULLS_FIRST;
        break;

      case substrait::SortField::SORT_DIRECTION_DESC_NULLS_LAST:
        dordertype = OrderType::DESCENDING;
        dnullorder = OrderByNullType::NULLS_LAST;
        break;

      default:
        throw InternalException("Unsupported ordering " + to_string(sordf.direction()));
    }

    return { dordertype, dnullorder, TranspileExpr(sordf.expr()) };
  }

  unique_ptr<LogicalOperator>
  DuckDBEnginePlan::TranspileJoinOp(const substrait::JoinRel& sjoin) {
    JoinType djointype = TranspileJoinType(sjoin);
    unique_ptr<ParsedExpression> join_condition = TranspileExpr(sjoin.expression());

    auto join_ndx = binder.GenerateTableIndex();
    auto join_op  = make_uniq<
    return make_shared<JoinRelation>(
       TranspileOp(sjoin.left())->Alias("left")
      ,TranspileOp(sjoin.right())->Alias("right")
      ,std::move(join_condition)
      ,djointype
    );
  }

  unique_ptr<LogicalOperator>
  DuckDBEnginePlan::TranspileCrossProductOp(const substrait::CrossRel& scross) {
    return make_shared<CrossProductRelation>(
       TranspileOp(scross.left())->Alias("left")
      ,TranspileOp(scross.right())->Alias("right")
    );
  }

  unique_ptr<LogicalOperator>
  DuckDBEnginePlan::TranspileFetchOp(const substrait::FetchRel& slimit) {
    return make_shared<LimitRelation>(
       TranspileOp(slimit.input())
      ,slimit.count()
      ,slimit.offset()
    );
  }

  unique_ptr<LogicalOperator>
  DuckDBEnginePlan::TranspileFilterOp(const substrait::FilterRel& sfilter) {
    return make_shared<FilterRelation>(
       TranspileOp(sfilter.input())
      ,TranspileExpr(sfilter.condition())
    );
  }

  unique_ptr<LogicalOperator>
  DuckDBEnginePlan::TranspileProjectOp(const substrait::ProjectRel& sproj) {
    vector<unique_ptr<ParsedExpression>> expressions;
    for (auto &sexpr : sproj.expressions()) {
      expressions.push_back(TranspileExpr(sexpr));
    }

    auto proj_ndx = binder->GenerateTableIndex();
    auto proj_op = make_uniq<LogicalProjection>(proj_ndx, std::move(expressions));
    proj_op->AddChild(std::move(TranspileOp(sproj.input())));

    return proj_op;
  }


  unique_ptr<LogicalOperator>
  DuckDBEnginePlan::TranspileAggregateOp(const substrait::AggregateRel& saggr) {
    vector<unique_ptr<ParsedExpression>> groups, expressions;

    if (saggr.groupings_size() > 0) {
      for (auto &sgrp : saggr.groupings()) {
        for (auto &sgrpexpr : sgrp.grouping_expressions()) {
          groups.push_back(TranspileExpr(sgrpexpr));
          expressions.push_back(TranspileExpr(sgrpexpr));
        }
      }
    }

    for (auto &smeas : saggr.measures()) {
      vector<unique_ptr<ParsedExpression>> children;
      for (auto &sarg : smeas.measure().arguments()) {
        children.push_back(TranspileExpr(sarg.value()));
      }

      auto function_name = FindFunction(smeas.measure().function_reference());
      if (function_name == "count" && children.empty()) {
        function_name = "count_star";
      }

      expressions.push_back(
        make_uniq<FunctionExpression>(
           RemapFunctionName(function_name), std::move(children)
        )
      );
    }

    return make_shared<AggregateRelation>(
       TranspileOp(saggr.input())
      ,std::move(expressions)
      ,std::move(groups)
    );
  }


  unique_ptr<LogicalOperator>
  DuckDBEnginePlan::TranspileReadOp(const substrait::ReadRel& sget) {
    unique_ptr<LogicalOperator> scan;

    // Find a table or view with given name
    if (sget.has_named_table()) {
      try         { scan = conn.Table(sget.named_table().names(0)); }
      catch (...) { scan = conn.View (sget.named_table().names(0)); }
    }

    // Otherwise, try scanning from list of parquet files
    else if (sget.has_local_files()) {
      vector<Value> parquet_files;

      auto local_file_items = sget.local_files().items();
      for (auto &current_file : local_file_items) {
        if (current_file.has_parquet()) {

          if (current_file.has_uri_file()) {
            parquet_files.emplace_back(current_file.uri_file());
          }

          else if (current_file.has_uri_path()) {
            parquet_files.emplace_back(current_file.uri_path());
          }

          else {
            throw NotImplementedException(
              "Unsupported type for file path, Only uri_file and uri_path are "
              "currently supported"
            );
          }
        }

        else {
          throw NotImplementedException(
            "Unsupported type of local file for read operator on substrait"
          );
        }
      }

      string name = "parquet_" + StringUtil::GenerateRandomName();
      named_parameter_map_t named_parameters({{"binary_as_string", Value::BOOLEAN(false)}});

      scan = conn.TableFunction(
        "parquet_scan", {Value::LIST(parquet_files)}, named_parameters
      )->Alias(name);
    }

    else {
      throw NotImplementedException("Unsupported type of read operator for substrait");
    }

    // Filter predicate for scan operation
    if (sget.has_filter()) {
      scan = make_shared<FilterRelation>(std::move(scan), TranspileExpr(sget.filter()));
    }

    // Projection predicate for scan operation
    if (sget.has_projection()) {
      vector<string>                       aliases;
      vector<unique_ptr<ParsedExpression>> expressions;

      idx_t expr_idx = 0;
      for (auto &sproj : sget.projection().select().struct_items()) {
        // FIXME how to get actually alias?
        aliases.push_back("expr_" + to_string(expr_idx++));

        // TODO make sure nothing else is in there
        expressions.push_back(make_uniq<PositionalReferenceExpression>(sproj.field() + 1));
      }

      scan = make_shared<ProjectionRelation>(
        std::move(scan), std::move(expressions), std::move(aliases)
      );
    }

    return scan;
  }


  unique_ptr<LogicalOperator>
  DuckDBEnginePlan::TranspileSortOp(const substrait::SortRel &ssort) {
    vector<OrderByNode> order_nodes;
    for (auto &sordf : ssort.sorts()) {
      order_nodes.push_back(TranspileOrder(sordf));
    }

    return make_shared<OrderRelation>(TranspileOp(ssort.input()), std::move(order_nodes));
  }


  unique_ptr<LogicalOperator>
  DuckDBEnginePlan::TranspileSetOp(const substrait::SetRel &sset) {
    // TODO: see if this is necessary for some cases
    // D_ASSERT(sop.has_set());

    auto  type   = TranspileSetOperationType(sset.op());
    auto& inputs = sset.inputs();
    if (sset.inputs_size() > 2) {
      throw NotImplementedException(
        "Too many inputs (%d) for this set operation", sset.inputs_size()
      );
    }

    auto lhs = TranspileOp(inputs[0]);
    auto rhs = TranspileOp(inputs[1]);
    return make_shared<SetOpRelation>(std::move(lhs), std::move(rhs), type);
  }


  //! Transpile Substrait Operations to DuckDB Relations
  using SRelType = substrait::Rel::RelTypeCase;
  unique_ptr<LogicalOperator> DuckDBEnginePlan::TranspileOp(const substrait::Rel& sop) {
    switch (sop.rel_type_case()) {
      case SRelType::kJoin:      return TranspileJoinOp        (sop.join());
      case SRelType::kCross:     return TranspileCrossProductOp(sop.cross());
      case SRelType::kFetch:     return TranspileFetchOp       (sop.fetch());
      case SRelType::kFilter:    return TranspileFilterOp      (sop.filter());
      case SRelType::kProject:   return TranspileProjectOp     (sop.project());
      case SRelType::kAggregate: return TranspileAggregateOp   (sop.aggregate());
      case SRelType::kRead:      return TranspileReadOp        (sop.read());
      case SRelType::kSort:      return TranspileSortOp        (sop.sort());
      case SRelType::kSet:       return TranspileSetOp         (sop.set());

      default:
        throw InternalException(
          "Unsupported relation type " + to_string(sop.rel_type_case())
        );
    }
  }


  //! Transpiles Substrait Plan Root To a DuckDB Relation
  unique_ptr<LogicalOperator>
  DuckDBEnginePlan::TranspileRootOp(const substrait::RelRoot& sop) {
    vector<unique_ptr<ParsedExpression>> expressions;

    int id = 1;
    for (auto &column_name : sop.names()) {
      expressions.push_back(make_uniq<ColumnRefExpression>(column_name));
    }

    auto proj_ndx = binder->GenerateTableIndex();
    auto proj_op = make_uniq<LogicalProjection>(proj_ndx, std::move(expressions));
    proj_op->AddChild(std::move(TranspileOp(sop.input())));

    return proj_op;
  }

} // namespace: duckdb
