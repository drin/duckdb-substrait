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

#include "plans.hpp"

#include "duckdb/main/relation/join_relation.hpp"
#include "duckdb/main/relation/cross_product_relation.hpp"
#include "duckdb/main/relation/limit_relation.hpp"
#include "duckdb/main/relation/projection_relation.hpp"
#include "duckdb/main/relation/setop_relation.hpp"
#include "duckdb/main/relation/aggregate_relation.hpp"
#include "duckdb/main/relation/filter_relation.hpp"
#include "duckdb/main/relation/order_relation.hpp"

#include "duckdb/parser/expression/list.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"

#include "duckdb/function/table_function.hpp"

#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/table_filter.hpp"

#include "custom_extensions/custom_extensions.hpp"

namespace duckdb {

  //! Convenience function to convert a substrait type to a logical duckdb type
  LogicalType SubstraitToDuckType(const substrait::Type& s_type);

} // namespace: duckdb
