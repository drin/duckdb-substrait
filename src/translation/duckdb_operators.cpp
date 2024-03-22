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
  TranslateSetOperationType(substrait::SetRel::SetOp setop) {
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


  static duckdb::JoinType 
  TranslateJoinType(const substrait::JoinRel& sjoin) {
    switch (sjoin.type()) {
      case substrait::JoinRel::JOIN_TYPE_INNER:  return duckdb::JoinType::INNER;
      case substrait::JoinRel::JOIN_TYPE_LEFT:   return duckdb::JoinType::LEFT;
      case substrait::JoinRel::JOIN_TYPE_RIGHT:  return duckdb::JoinType::RIGHT;
      case substrait::JoinRel::JOIN_TYPE_SINGLE: return duckdb::JoinType::SINGLE;
      case substrait::JoinRel::JOIN_TYPE_SEMI:   return duckdb::JoinType::SEMI;

      default:
        throw InternalException("Unsupported join type");
    }
  }


  OrderByNode
  DuckDBTranslator::TranslateOrder(const substrait::SortField& sordf) {
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

    return { dordertype, dnullorder, TranslateExpr(sordf.expr()) };
  }

  shared_ptr<Relation>
  DuckDBTranslator::TranslateJoinOp(const substrait::JoinRel& sjoin) {
    JoinType djointype = TranslateJoinType(sjoin);
    unique_ptr<ParsedExpression> join_condition = TranslateExpr(sjoin.expression());

    return make_shared<JoinRelation>(
       TranslateOp(sjoin.left())->Alias("left")
      ,TranslateOp(sjoin.right())->Alias("right")
      ,std::move(join_condition)
      ,djointype
    );
  }

  shared_ptr<Relation>
  DuckDBTranslator::TranslateCrossProductOp(const substrait::CrossRel& scross) {
    return make_shared<CrossProductRelation>(
       TranslateOp(scross.left())->Alias("left")
      ,TranslateOp(scross.right())->Alias("right")
    );
  }

  shared_ptr<Relation>
  DuckDBTranslator::TranslateFetchOp(const substrait::FetchRel& slimit) {
    return make_shared<LimitRelation>(
       TranslateOp(slimit.input())
      ,slimit.count()
      ,slimit.offset()
    );
  }

  shared_ptr<Relation>
  DuckDBTranslator::TranslateFilterOp(const substrait::FilterRel& sfilter) {
    return make_shared<FilterRelation>(
       TranslateOp(sfilter.input())
      ,TranslateExpr(sfilter.condition())
    );
  }

  shared_ptr<Relation>
  DuckDBTranslator::TranslateProjectOp(const substrait::ProjectRel& sproj) {
    vector<unique_ptr<ParsedExpression>> expressions;
    for (auto &sexpr : sproj.expressions()) {
      expressions.push_back(TranslateExpr(sexpr));
    }

    vector<string> mock_aliases;
    for (size_t i = 0; i < expressions.size(); i++) {
      mock_aliases.push_back("expr_" + to_string(i));
    }

    return make_shared<ProjectionRelation>(
       TranslateOp(sproj.input())
      ,std::move(expressions)
      ,std::move(mock_aliases)
    );
  }


  shared_ptr<Relation>
  DuckDBTranslator::TranslateAggregateOp(const substrait::AggregateRel& saggr) {
    vector<unique_ptr<ParsedExpression>> groups, expressions;

    if (saggr.groupings_size() > 0) {
      for (auto &sgrp : saggr.groupings()) {
        for (auto &sgrpexpr : sgrp.grouping_expressions()) {
          groups.push_back(TranslateExpr(sgrpexpr));
          expressions.push_back(TranslateExpr(sgrpexpr));
        }
      }
    }

    for (auto &smeas : saggr.measures()) {
      vector<unique_ptr<ParsedExpression>> children;
      for (auto &sarg : smeas.measure().arguments()) {
        children.push_back(TranslateExpr(sarg.value()));
      }

      auto function_id   = smeas.measure().function_reference();
      auto function_name = functions_map->FindExtensionFunction(function_id);
      if (function_name == "count" && children.empty()) { function_name = "count_star"; }

      expressions.push_back(
        make_uniq<FunctionExpression>(
           RemapFunctionName(function_name), std::move(children)
        )
      );
    }

    return make_shared<AggregateRelation>(
       TranslateOp(saggr.input())
      ,std::move(expressions)
      ,std::move(groups)
    );
  }


  shared_ptr<Relation>
  DuckDBTranslator::TranslateReadOp(const substrait::ReadRel& sget) {
    shared_ptr<Relation> scan;
    // Find a table or view with given name
    if (sget.has_named_table()) {
      try         { scan = t_conn->Table(sget.named_table().names(0)); }
      catch (...) { scan = t_conn->View (sget.named_table().names(0)); }
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

      scan = t_conn->TableFunction( "parquet_scan"
                                   ,{Value::LIST(parquet_files)}
                                   ,named_parameters
                                  )->Alias(name);
    }

    else {
      throw NotImplementedException("Unsupported type of read operator for substrait");
    }

    // Filter predicate for scan operation
    if (sget.has_filter()) {
      scan = make_shared<FilterRelation>(std::move(scan), TranslateExpr(sget.filter()));
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
        expressions.push_back(
          make_uniq<PositionalReferenceExpression>(sproj.field() + 1)
        );
      }

      scan = make_shared<ProjectionRelation>(
        std::move(scan), std::move(expressions), std::move(aliases)
      );
    }

    return scan;
  }


  shared_ptr<Relation>
  DuckDBTranslator::TranslateSortOp(const substrait::SortRel &ssort) {
    vector<OrderByNode> order_nodes;
    for (auto &sordf : ssort.sorts()) {
      order_nodes.push_back(TranslateOrder(sordf));
    }

    return make_shared<OrderRelation>(TranslateOp(ssort.input()), std::move(order_nodes));
  }


  shared_ptr<Relation>
  DuckDBTranslator::TranslateSetOp(const substrait::SetRel &sset) {
    // TODO: see if this is necessary for some cases
    // D_ASSERT(sop.has_set());

    auto  type   = TranslateSetOperationType(sset.op());
    auto& inputs = sset.inputs();
    if (sset.inputs_size() > 2) {
      throw NotImplementedException(
        "Too many inputs (%d) for this set operation", sset.inputs_size()
      );
    }

    auto lhs = TranslateOp(inputs[0]);
    auto rhs = TranslateOp(inputs[1]);
    return make_shared<SetOpRelation>(std::move(lhs), std::move(rhs), type);
  }


  //! Translate Substrait Operations to DuckDB Relations
  using SRelType = substrait::Rel::RelTypeCase;
  shared_ptr<Relation> DuckDBTranslator::TranslateOp(const substrait::Rel& sop) {
    switch (sop.rel_type_case()) {
      case SRelType::kJoin:      return TranslateJoinOp        (sop.join());
      case SRelType::kCross:     return TranslateCrossProductOp(sop.cross());
      case SRelType::kFetch:     return TranslateFetchOp       (sop.fetch());
      case SRelType::kFilter:    return TranslateFilterOp      (sop.filter());
      case SRelType::kProject:   return TranslateProjectOp     (sop.project());
      case SRelType::kAggregate: return TranslateAggregateOp   (sop.aggregate());
      case SRelType::kRead:      return TranslateReadOp        (sop.read());
      case SRelType::kSort:      return TranslateSortOp        (sop.sort());
      case SRelType::kSet:       return TranslateSetOp         (sop.set());

      default:
        throw InternalException(
          "Unsupported relation type " + to_string(sop.rel_type_case())
        );
    }
  }


  //! Translates Substrait Plan Root To a DuckDB Relation
  shared_ptr<Relation> DuckDBTranslator::TranslateRootOp(const substrait::RelRoot& sop) {
    vector<string> aliases;
    vector<unique_ptr<ParsedExpression>> expressions;

    int id = 1;
    for (auto &column_name : sop.names()) {
      aliases.push_back(column_name);
      expressions.push_back(make_uniq<PositionalReferenceExpression>(id++));
    }

    return make_shared<ProjectionRelation>(
      TranslateOp(sop.input()), std::move(expressions), aliases
    );
  }


  // >> Entry points into the functions implemented here
  unique_ptr<DuckSystemPlan>
  DuckDBTranslator::TranslatePlanMessage(const string& serialized_msg) {
    auto plan = mohair::SubstraitPlanFromSubstraitMessage(serialized_msg);
    functions_map->RegisterExtensionFunctions(*plan);

    auto engine_plan = TranslateRootOp(plan->relations(0).root());
    return make_uniq<DuckSystemPlan>(std::move(plan), engine_plan);
  }

  unique_ptr<DuckSystemPlan>
  DuckDBTranslator::TranslatePlanJson(const string& json_msg) {
    auto plan        = mohair::SubstraitPlanFromSubstraitJson(json_msg);
    functions_map->RegisterExtensionFunctions(*plan);

    auto engine_plan = TranslateRootOp(plan->relations(0).root());
    return make_uniq<DuckSystemPlan>(std::move(plan), engine_plan);
  }

} // namespace: duckdb
