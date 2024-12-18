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
// Aliases

namespace skysubstrait = skyproto::substrait;


// ------------------------------
// Functions

namespace duckdb {

  LogicalType SubstraitToDuckType(const skysubstrait::Type& s_type) {
    if      (s_type.has_bool_()) { return LogicalType(LogicalTypeId::BOOLEAN ); }
    else if (s_type.has_i16()  ) { return LogicalType(LogicalTypeId::SMALLINT); }
    else if (s_type.has_i32()  ) { return LogicalType(LogicalTypeId::INTEGER ); }
    else if (s_type.has_i64()  ) { return LogicalType(LogicalTypeId::BIGINT  ); }
    else if (s_type.has_date() ) { return LogicalType(LogicalTypeId::DATE    ); }
    else if (s_type.has_fp64() ) { return LogicalType(LogicalTypeId::DOUBLE  ); }

    else if (s_type.has_varchar() || s_type.has_string()) {
      return LogicalType(LogicalTypeId::VARCHAR);
    }

    else if (s_type.has_decimal()) {
      auto &s_decimal_type = s_type.decimal();
      return LogicalType::DECIMAL(s_decimal_type.precision(), s_decimal_type.scale());
    }

    else {
      throw InternalException("Substrait type not yet supported");
    }
  }

  using SLiteralType = skysubstrait::Expression::Literal::LiteralTypeCase;

  unique_ptr<ParsedExpression>
  DuckDBTranslator::TranslateLiteralExpr(const skysubstrait::Expression::Literal& slit) {
    Value dval;

    if (slit.has_null()) {
      dval = Value(LogicalType::SQLNULL);
      return make_uniq<ConstantExpression>(dval);
    }

    switch (slit.literal_type_case()) {
      case SLiteralType::kFp64:
        dval = Value::DOUBLE(slit.fp64());
        break;

      case SLiteralType::kFp32:
        dval = Value::FLOAT(slit.fp32());
        break;

      case SLiteralType::kString:
        dval = Value(slit.string());
        break;

      case SLiteralType::kDecimal: {
        const auto& sdecimal = slit.decimal();
        auto decimal_type = LogicalType::DECIMAL(
           sdecimal.precision()
          ,sdecimal.scale()
        );

        hugeint_t substrait_value;
        auto raw_value = (uint64_t *) sdecimal.value().c_str();
        substrait_value.lower = raw_value[0];
        substrait_value.upper = raw_value[1];
        Value val = Value::HUGEINT(substrait_value);

        // cast to correct value
        switch (decimal_type.InternalType()) {
          case PhysicalType::INT8:
            dval = Value::DECIMAL(
               val.GetValue<int8_t>()
              ,sdecimal.precision()
              ,sdecimal.scale()
            );
            break;

          case PhysicalType::INT16:
            dval = Value::DECIMAL(
               val.GetValue<int16_t>()
              ,sdecimal.precision()
              ,sdecimal.scale()
            );
            break;

          case PhysicalType::INT32:
            dval = Value::DECIMAL(
               val.GetValue<int32_t>()
              ,sdecimal.precision()
              ,sdecimal.scale()
            );
            break;

          case PhysicalType::INT64:
            dval = Value::DECIMAL(
               val.GetValue<int64_t>()
              ,sdecimal.precision()
              ,sdecimal.scale()
            );
            break;

          case PhysicalType::INT128:
            dval = Value::DECIMAL(substrait_value, sdecimal.precision(), sdecimal.scale());
            break;

          default:
            throw InternalException("Not accepted internal type for decimal");
        }
        break;
      }

      case SLiteralType::kBoolean: {
        dval = Value(slit.boolean());
        break;
      }

      case SLiteralType::kI8:
        dval = Value::TINYINT(slit.i8());
        break;

      case SLiteralType::kI32:
        dval = Value::INTEGER(slit.i32());
        break;

      case SLiteralType::kI64:
        dval = Value::BIGINT(slit.i64());
        break;

      case SLiteralType::kDate: {
        date_t date(slit.date());
        dval = Value::DATE(date);
        break;
      }

      case SLiteralType::kTime: {
        dtime_t time(slit.time());
        dval = Value::TIME(time);
        break;
      }

      case SLiteralType::kIntervalYearToMonth: {
        interval_t interval;
        interval.months = slit.interval_year_to_month().months();
        interval.days = 0;
        interval.micros = 0;
        dval = Value::INTERVAL(interval);
        break;
      }

      case SLiteralType::kIntervalDayToSecond: {
        interval_t interval;
        interval.months = 0;
        interval.days = slit.interval_day_to_second().days();
        interval.micros = slit.interval_day_to_second().microseconds();
        dval = Value::INTERVAL(interval);
        break;
      }

      default:
        throw InternalException(to_string(slit.literal_type_case()));
    }

    return make_uniq<ConstantExpression>(dval);
  }


  unique_ptr<ParsedExpression>
  DuckDBTranslator::TranslateSelectionExpr(const skysubstrait::Expression &sexpr) {
    if (   !sexpr.selection().has_direct_reference()
        || !sexpr.selection().direct_reference().has_struct_field()) {
      throw InternalException("Can only have direct struct references in selections");
    }

    return make_uniq<PositionalReferenceExpression>(
      sexpr.selection().direct_reference().struct_field().field() + 1
    );
  }

  unique_ptr<ParsedExpression>
  DuckDBTranslator::TranslateScalarFunctionExpr(const skysubstrait::Expression& sexpr) {
    auto function_id   = sexpr.scalar_function().function_reference();
    auto function_name = functions_map->FindExtensionFunction(function_id);
    function_name      = RemoveExtension(function_name);

    vector<unique_ptr<ParsedExpression>> children;
    vector<string>                       enum_expressions;

    auto& function_arguments = sexpr.scalar_function().arguments();
    for (auto& sarg : function_arguments) {

      // value expression
      if (sarg.has_value()) { children.push_back(TranslateExpr(sarg.value())); }

      // type expression
      else if (sarg.has_type()) {
        throw NotImplementedException(
          "Type arguments in Substrait expressions are not supported yet!"
        );
      }

      // enum expression
      else {
        D_ASSERT(sarg.has_enum_());
        auto &enum_str = sarg.enum_();
        enum_expressions.push_back(enum_str);
      }
    }

    // string compare galore
    // TODO simplify this
    if (function_name == "and") {
      return make_uniq<ConjunctionExpression>(
        ExpressionType::CONJUNCTION_AND, std::move(children)
      );
    }

    else if (function_name == "or") {
      return make_uniq<ConjunctionExpression>(
        ExpressionType::CONJUNCTION_OR, std::move(children)
      );
    }

    else if (function_name == "lt") {
      D_ASSERT(children.size() == 2);
      return make_uniq<ComparisonExpression>(
         ExpressionType::COMPARE_LESSTHAN
        ,std::move(children[0])
        ,std::move(children[1])
      );
    }

    else if (function_name == "equal") {
      D_ASSERT(children.size() == 2);
      return make_uniq<ComparisonExpression>(
         ExpressionType::COMPARE_EQUAL
        ,std::move(children[0])
        ,std::move(children[1])
      );
    }

    else if (function_name == "not_equal") {
      D_ASSERT(children.size() == 2);
      return make_uniq<ComparisonExpression>(
         ExpressionType::COMPARE_NOTEQUAL
        ,std::move(children[0])
        ,std::move(children[1])
      );
    }

    else if (function_name == "lte") {
      D_ASSERT(children.size() == 2);
      return make_uniq<ComparisonExpression>(
         ExpressionType::COMPARE_LESSTHANOREQUALTO
        ,std::move(children[0])
        ,std::move(children[1])
      );
    }

    else if (function_name == "gte") {
      D_ASSERT(children.size() == 2);
      return make_uniq<ComparisonExpression>(
         ExpressionType::COMPARE_GREATERTHANOREQUALTO
        ,std::move(children[0])
        ,std::move(children[1])
      );
    }

    else if (function_name == "gt") {
      D_ASSERT(children.size() == 2);
      return make_uniq<ComparisonExpression>(
         ExpressionType::COMPARE_GREATERTHAN
        ,std::move(children[0])
        ,std::move(children[1])
      );
    }

    else if (function_name == "is_not_null") {
      D_ASSERT(children.size() == 1);
      return make_uniq<OperatorExpression>(
         ExpressionType::OPERATOR_IS_NOT_NULL
        ,std::move(children[0])
      );
    }

    else if (function_name == "is_null") {
      D_ASSERT(children.size() == 1);
      return make_uniq<OperatorExpression>(
         ExpressionType::OPERATOR_IS_NULL
        ,std::move(children[0])
      );
    }

    else if (function_name == "not") {
      D_ASSERT(children.size() == 1);
      return make_uniq<OperatorExpression>(
         ExpressionType::OPERATOR_NOT
        ,std::move(children[0])
      );
    }

    else if (function_name == "is_not_distinct_from") {
      D_ASSERT(children.size() == 2);
      return make_uniq<ComparisonExpression>(
         ExpressionType::COMPARE_NOT_DISTINCT_FROM
        ,std::move(children[0])
        ,std::move(children[1])
      );
    }

    else if (function_name == "between") {
      // FIXME: ADD between to substrait extension
      D_ASSERT(children.size() == 3);
      return make_uniq<BetweenExpression>(
         std::move(children[0])
        ,std::move(children[1])
        ,std::move(children[2])
      );
    }

    else if (function_name == "extract") {
      D_ASSERT(enum_expressions.size() == 1);

      auto& subfield = enum_expressions[0];
      AssertValidDateSubfield(subfield);

      auto constant_expression = make_uniq<ConstantExpression>(Value(subfield));
      children.insert(children.begin(), std::move(constant_expression));
    }

    return make_uniq<FunctionExpression>(
      RemapFunctionName(function_name), std::move(children)
    );
  }


  unique_ptr<ParsedExpression>
  DuckDBTranslator::TranslateIfThenExpr(const skysubstrait::Expression &sexpr) {
    const auto& scase = sexpr.if_then();
    auto        dcase = make_uniq<CaseExpression>();

    for (const auto &sif : scase.ifs()) {
      CaseCheck dif;
      dif.when_expr = TranslateExpr(sif.if_());
      dif.then_expr = TranslateExpr(sif.then());
      dcase->case_checks.push_back(std::move(dif));
    }

    dcase->else_expr = TranslateExpr(scase.else_());
    return std::move(dcase);
  }


  unique_ptr<ParsedExpression>
  DuckDBTranslator::TranslateCastExpr(const skysubstrait::Expression &sexpr) {
    const auto& scast      = sexpr.cast();
    auto        cast_type  = SubstraitToDuckType(scast.type());
    auto        cast_child = TranslateExpr(scast.input());

    return make_uniq<CastExpression>(cast_type, std::move(cast_child));
  }


  unique_ptr<ParsedExpression>
  DuckDBTranslator::TranslateInExpr(const skysubstrait::Expression& sexpr) {
    const auto &substrait_in = sexpr.singular_or_list();

    vector<unique_ptr<ParsedExpression>> values;
    values.emplace_back(TranslateExpr(substrait_in.value()));

    for (idx_t i = 0; i < (idx_t)substrait_in.options_size(); i++) {
      values.emplace_back(TranslateExpr(substrait_in.options(i)));
    }

    return make_uniq<OperatorExpression>(
      ExpressionType::COMPARE_IN, std::move(values)
    );
  }


  // >> Top-level translation function
  using SExprType = skysubstrait::Expression::RexTypeCase;

  unique_ptr<ParsedExpression>
  DuckDBTranslator::TranslateExpr(const skysubstrait::Expression& sexpr) {
    switch (sexpr.rex_type_case()) {
      case SExprType::kLiteral:        return TranslateLiteralExpr       (sexpr.literal());
      case SExprType::kSelection:      return TranslateSelectionExpr     (sexpr);
      case SExprType::kScalarFunction: return TranslateScalarFunctionExpr(sexpr);
      case SExprType::kIfThen:         return TranslateIfThenExpr        (sexpr);
      case SExprType::kCast:           return TranslateCastExpr          (sexpr);
      case SExprType::kSingularOrList: return TranslateInExpr            (sexpr);
      case SExprType::kSubquery:
      default:
        throw InternalException(
          "Unsupported expression type " + to_string(sexpr.rex_type_case())
        );
    }
  }

} // namespace: duckdb
