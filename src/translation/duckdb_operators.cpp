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

namespace skysubstrait = skytether::substrait;
namespace skymohair    = skytether::mohair;

using NamedTable = skysubstrait::ReadRel::NamedTable;
using LocalFiles = skysubstrait::ReadRel::LocalFiles;


// ------------------------------
// Functions

namespace duckdb {

  //! Normalization of an Arrow URI path to a table name
  string TableAliasForArrowFile(const std::string& uri_path) {
    string table_alias { uri_path };

    // NOTE: replace system characters with ones that play nice at a higher level
    for (uint32_t ndx = 0; ndx < table_alias.size(); ++ndx) {
      // TODO: can't do string literal comparisons
      if      (table_alias[ndx] == '/') { table_alias[ndx] = '.'; }
      else if (table_alias[ndx] == ';') { table_alias[ndx] = '-'; }
    }

    return table_alias;
  }

  static duckdb::SetOperationType
  TranslateSetOperationType(skysubstrait::SetRel::SetOp setop) {
    switch (setop) {
      case skysubstrait::SetRel::SET_OP_UNION_ALL: {
        return duckdb::SetOperationType::UNION;
      }

      case skysubstrait::SetRel::SET_OP_MINUS_PRIMARY: {
        return duckdb::SetOperationType::EXCEPT;
      }

      case skysubstrait::SetRel::SET_OP_INTERSECTION_PRIMARY: {
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
  TranslateJoinType(const skysubstrait::JoinRel& sjoin) {
    switch (sjoin.type()) {
      case skysubstrait::JoinRel::JOIN_TYPE_INNER:       return duckdb::JoinType::INNER;
      case skysubstrait::JoinRel::JOIN_TYPE_LEFT:        return duckdb::JoinType::LEFT;
      case skysubstrait::JoinRel::JOIN_TYPE_RIGHT:       return duckdb::JoinType::RIGHT;
      case skysubstrait::JoinRel::JOIN_TYPE_LEFT_SINGLE: return duckdb::JoinType::SINGLE;
      case skysubstrait::JoinRel::JOIN_TYPE_LEFT_SEMI:   return duckdb::JoinType::SEMI;
      case skysubstrait::JoinRel::JOIN_TYPE_OUTER:       return duckdb::JoinType::OUTER;

      default:
        throw InternalException("Unsupported join type");
    }
  }


  OrderByNode
  DuckDBTranslator::TranslateOrder(const skysubstrait::SortField& sordf) {
    OrderType       dordertype;
    OrderByNullType dnullorder;

    switch (sordf.direction()) {
      case skysubstrait::SortField::SORT_DIRECTION_ASC_NULLS_FIRST:
        dordertype = OrderType::ASCENDING;
        dnullorder = OrderByNullType::NULLS_FIRST;
        break;

      case skysubstrait::SortField::SORT_DIRECTION_ASC_NULLS_LAST:
        dordertype = OrderType::ASCENDING;
        dnullorder = OrderByNullType::NULLS_LAST;
        break;

      case skysubstrait::SortField::SORT_DIRECTION_DESC_NULLS_FIRST:
        dordertype = OrderType::DESCENDING;
        dnullorder = OrderByNullType::NULLS_FIRST;
        break;

      case skysubstrait::SortField::SORT_DIRECTION_DESC_NULLS_LAST:
        dordertype = OrderType::DESCENDING;
        dnullorder = OrderByNullType::NULLS_LAST;
        break;

      default:
        throw InternalException("Unsupported ordering " + to_string(sordf.direction()));
    }

    return { dordertype, dnullorder, TranslateExpr(sordf.expr()) };
  }

  shared_ptr<Relation>
  DuckDBTranslator::TranslateJoinOp(const skysubstrait::JoinRel& sjoin) {
    JoinType djointype = TranslateJoinType(sjoin);
    unique_ptr<ParsedExpression> join_condition = TranslateExpr(sjoin.expression());

    return make_shared_ptr<JoinRelation>(
       TranslateOp(sjoin.left())->Alias("left")
      ,TranslateOp(sjoin.right())->Alias("right")
      ,std::move(join_condition)
      ,djointype
    );
  }

  shared_ptr<Relation>
  DuckDBTranslator::TranslateCrossProductOp(const skysubstrait::CrossRel& scross) {
    return make_shared_ptr<CrossProductRelation>(
       TranslateOp(scross.left())->Alias("left")
      ,TranslateOp(scross.right())->Alias("right")
    );
  }

  shared_ptr<Relation>
  DuckDBTranslator::TranslateFetchOp(const skysubstrait::FetchRel& slimit) {
    return make_shared_ptr<LimitRelation>(
       TranslateOp(slimit.input())
      ,slimit.count()
      ,slimit.offset()
    );
  }

  shared_ptr<Relation>
  DuckDBTranslator::TranslateFilterOp(const skysubstrait::FilterRel& sfilter) {
    return make_shared_ptr<FilterRelation>(
       TranslateOp(sfilter.input())
      ,TranslateExpr(sfilter.condition())
    );
  }

  shared_ptr<Relation>
  DuckDBTranslator::TranslateProjectOp(const skysubstrait::ProjectRel& sproj) {
    vector<unique_ptr<ParsedExpression>> expressions;
    for (auto &sexpr : sproj.expressions()) {
      expressions.push_back(TranslateExpr(sexpr));
    }

    vector<string> mock_aliases;
    for (size_t i = 0; i < expressions.size(); i++) {
      mock_aliases.push_back("expr_" + to_string(i));
    }

    return make_shared_ptr<ProjectionRelation>(
       TranslateOp(sproj.input())
      ,std::move(expressions)
      ,std::move(mock_aliases)
    );
  }


  shared_ptr<Relation>
  DuckDBTranslator::TranslateAggregateOp(const skysubstrait::AggregateRel& saggr) {
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

    return make_shared_ptr<AggregateRelation>(
       TranslateOp(saggr.input())
      ,std::move(expressions)
      ,std::move(groups)
    );
  }


  shared_ptr<Relation>
  DuckDBTranslator::ScanNamedTable(const NamedTable& named_table) {
      try         { return t_conn->Table(named_table.names(0)); }
      catch (...) { return t_conn->View (named_table.names(0)); }
  }


  shared_ptr<Relation>
  DuckDBTranslator::ScanFileListParquet(const LocalFiles& local_files) {
    vector<Value> parquet_files;

    auto& local_file_items = local_files.items();
    for (auto &current_file : local_file_items) {
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

    string scan_alias {
      "parquet_" + StringUtil::GenerateRandomName()
    };

    named_parameter_map_t named_parameters({
      { "binary_as_string", Value::BOOLEAN(false) }
    });

    return (
      t_conn->TableFunction(
                 "parquet_scan", {Value::LIST(parquet_files)}, named_parameters
              )
            ->Alias(scan_alias)
    );
  }


  shared_ptr<Relation>
  DuckDBTranslator::ScanFileListArrow(const LocalFiles& local_files) {
    vector<Value> arrow_files;
    string        table_name;

    auto& local_file_items = local_files.items();
    for (auto &current_file : local_file_items) {
      if (current_file.has_uri_path()) {
        arrow_files.emplace_back(current_file.uri_path());
        table_name = TableAliasForArrowFile(current_file.uri_path());
      }

      else {
        throw NotImplementedException("Only uri_path is supported for arrow file paths");
      }
    }

    // We expect the arrow files to contain arrow stream formatted data
    return (
      t_conn->TableFunction("scan_arrows_file", {Value::LIST(arrow_files)})
            ->Alias(table_name)
    );
  }


  using SFileFormatType = LocalFiles::FileOrFiles::FileFormatCase;
  shared_ptr<Relation>
  DuckDBTranslator::ScanFileList(const LocalFiles& local_files) {
    // TODO: currently, all files must be the same format
    switch (local_files.items(0).file_format_case()) {
      case SFileFormatType::kParquet:
        return ScanFileListParquet(local_files);
        break;

      case SFileFormatType::kArrow:
        return ScanFileListArrow(local_files);
        break;

      case SFileFormatType::kExtension:
      default:
        throw duckdb::NotImplementedException(
          "[test] Unsupported type of local file for read operator on substrait"
        );
    }
  }


  shared_ptr<Relation>
  DuckDBTranslator::TranslateReadOp(const skysubstrait::ReadRel& sget) {

    // Construct a scan relation based on the ReadRel's source type
    shared_ptr<Relation> scan;
    if      (sget.has_named_table()) { scan = ScanNamedTable(sget.named_table()); }
    else if (sget.has_local_files()) { scan =   ScanFileList(sget.local_files()); }
    else {
      throw NotImplementedException("Unsupported type of read operator for substrait");
    }

    // Filter predicate for scan operation
    if (sget.has_filter()) {
      scan = make_shared_ptr<FilterRelation>(std::move(scan), TranslateExpr(sget.filter()));
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

      scan = make_shared_ptr<ProjectionRelation>(
        std::move(scan), std::move(expressions), std::move(aliases)
      );
    }

    return scan;
  }


  shared_ptr<Relation>
  DuckDBTranslator::TranslateSortOp(const skysubstrait::SortRel &ssort) {
    vector<OrderByNode> order_nodes;
    for (auto &sordf : ssort.sorts()) {
      order_nodes.push_back(TranslateOrder(sordf));
    }

    return make_shared_ptr<OrderRelation>(TranslateOp(ssort.input()), std::move(order_nodes));
  }


  shared_ptr<Relation>
  DuckDBTranslator::TranslateSetOp(const skysubstrait::SetRel &sset) {
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
    return make_shared_ptr<SetOpRelation>(std::move(lhs), std::move(rhs), type);
  }


  //! A SkyRel is assumed to be a lookup against a skytether catalog
  shared_ptr<Relation>
  DuckDBTranslator::TranslateSkyRel(const skymohair::SkyRel& sky_rel) {
    string sky_relname { sky_rel.domain() + "/" + sky_rel.partition() };

    try         { return t_conn->Table(sky_relname); }
    catch (...) { return t_conn->View (sky_relname); }
  }

  //! A SkyPartitionRel is assumed to be a filesystem lookup against a partition (need to
  //  find names of each slice contained in the partition).
  shared_ptr<Relation>
  DuckDBTranslator::TranslateSkyPartitionRel(const skymohair::SkyPartitionRel& sky_rel) {
    vector<Value> slice_names;
    string table_name { sky_rel.domain() + "/" + sky_rel.partition() };

    // In this case, we iterate over slices() to get slice indices
    if (not sky_rel.slices().empty()) {
      for (int slice_id = 0; slice_id < sky_rel.slices_size(); ++slice_id) {
        slice_names.emplace_back(table_name + "-" + std::to_string(sky_rel.slices(slice_id)));
      }
    }

    // In this case, we generate slice indices up to the last index
    // NOTE: hardcoded for now due to lazyness
    else {
      for (uint32_t slice_ndx = 0; slice_ndx < 10; ++slice_ndx) {
        slice_names.emplace_back(table_name + "-" + std::to_string(slice_ndx));
      }
    }

    // We expect the arrow files to contain arrow stream formatted data
    return (
      t_conn->TableFunction("scan_arrows_file", {Value::LIST(slice_names)})
            ->Alias(table_name)
    );
  }

  //! A SkySliceRel is assumed to be a filesystem lookup against a single slice ("as-is").
  shared_ptr<Relation>
  DuckDBTranslator::TranslateSkySliceRel(const skymohair::SkySliceRel& sky_rel) {
    vector<Value> slice_names;
    slice_names.emplace_back(sky_rel.slice_key());

    // We expect the arrow files to contain arrow stream formatted data
    string table_name { sky_rel.domain() + "/" + sky_rel.partition() };
    return (
      t_conn->TableFunction("scan_arrows_file", {Value::LIST(slice_names)})
            ->Alias(table_name)
    );
  }

  shared_ptr<Relation>
  DuckDBTranslator::TranslateExtensionLeafOp(const skysubstrait::ExtensionLeafRel& leaf_rel) {
    shared_ptr<Relation> translated_rel { nullptr };

    if (not leaf_rel.has_detail()) {
      throw InternalException("ExtensionLeaf op is missing extension details");
    }

    // Figure out which type of extension operator it is
    auto& extension_msg = leaf_rel.detail();
    if (extension_msg.Is<skymohair::SkyRel>()) { 
      skymohair::SkyRel sky_rel;
      extension_msg.UnpackTo(&sky_rel);

      translated_rel = TranslateSkyRel(sky_rel);
    }

    else if (extension_msg.Is<skymohair::SkyPartitionRel>()) { 
      skymohair::SkyPartitionRel sky_rel;
      extension_msg.UnpackTo(&sky_rel);

      translated_rel = TranslateSkyPartitionRel(sky_rel);
    }

    else if (extension_msg.Is<skymohair::SkySliceRel>()) { 
      skymohair::SkySliceRel sky_rel;
      extension_msg.UnpackTo(&sky_rel);

      translated_rel = TranslateSkySliceRel(sky_rel);
    }

    // If a translator was matched and it succeeded, return the translated sub-plan
    if (translated_rel != nullptr) { return translated_rel; }

    // Otherwise, throw an exception
    std::cerr << "Unsupported extension type: " << extension_msg.descriptor()->name() << std::endl;
    throw InternalException("Unsupported extension type");
  }

  //! Translate Substrait Operations to DuckDB Relations
  using SRelType = skysubstrait::Rel::RelTypeCase;
  shared_ptr<Relation> DuckDBTranslator::TranslateOp(const skysubstrait::Rel& sop) {
    switch (sop.rel_type_case()) {
      case SRelType::kJoin:          return TranslateJoinOp         (sop.join());
      case SRelType::kCross:         return TranslateCrossProductOp (sop.cross());
      case SRelType::kFetch:         return TranslateFetchOp        (sop.fetch());
      case SRelType::kFilter:        return TranslateFilterOp       (sop.filter());
      case SRelType::kProject:       return TranslateProjectOp      (sop.project());
      case SRelType::kAggregate:     return TranslateAggregateOp    (sop.aggregate());
      case SRelType::kRead:          return TranslateReadOp         (sop.read());
      case SRelType::kSort:          return TranslateSortOp         (sop.sort());
      case SRelType::kSet:           return TranslateSetOp          (sop.set());
      case SRelType::kExtensionLeaf: return TranslateExtensionLeafOp(sop.extension_leaf());
      default:
        throw InternalException(
          "Unsupported relation type " + to_string(sop.rel_type_case())
        );
    }
  }


  //! Translates Substrait Plan Root To a DuckDB Relation
  shared_ptr<Relation> DuckDBTranslator::TranslateRootOp(const skysubstrait::RelRoot& sop) {
    vector<string> aliases;
    vector<unique_ptr<ParsedExpression>> expressions;

    int id = 1;
    for (auto &column_name : sop.names()) {
      aliases.push_back(column_name);
      expressions.push_back(make_uniq<PositionalReferenceExpression>(id++));
    }

    return make_shared_ptr<ProjectionRelation>(
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
