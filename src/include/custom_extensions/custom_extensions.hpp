//===----------------------------------------------------------------------===//
//                         DuckDB
//
// custom_extensions/substrait_custom_extensions.hpp
//
//
//===----------------------------------------------------------------------===//


// ------------------------------
// Dependencies
#pragma once

#include <unordered_map>
#include <substrait/type.pb.h>

#include "duckdb/common/types/hash.hpp"


// ------------------------------
// Dependencies

namespace duckdb {

  // >> Type forwards
  struct SubstraitCustomFunction;


  // >> Hash functors
  //! Hash functor for "*" (any arguments) substrait functions
  struct HashSubstraitFunctionsName {
    size_t operator()(SubstraitCustomFunction const& custom_function) const noexcept {
      return Hash(custom_function.name.c_str());
    }
  };

  //! Hash functor for "?" (repeatable argument) and regular substrait functions
  struct HashSubstraitFunctions {
    size_t operator()(SubstraitCustomFunction const& custom_function) const noexcept {
      // Hash the function arguments
      auto& fn_argtypes = custom_function.arg_types;

      auto hashed_argtypes = Hash(fn_argtypes[0].c_str());
      for (idx_t arg_ndx = 1; arg_ndx < fn_argtypes.size(); arg_ndx++) {
        hashed_argtypes = CombineHash(hashed_argtypes, Hash(fn_argtypes[arg_ndx].c_str()));
      }

      // Combine hashes for the function name and its argument types
      auto hashed_name = Hash(custom_function.name.c_str());
      return CombineHash(hashed_name, hashed_argtypes);
    }
  };


  // >> Classes
  //! Class to describe an individual substrait function
  struct SubstraitCustomFunction {

    // Constructors
    SubstraitCustomFunction() = default;
    SubstraitCustomFunction(string name_p, vector<string> arg_types_p)
        : name(std::move(name_p)), arg_types(std::move(arg_types_p)) {}

    // Functions
    bool operator==(const SubstraitCustomFunction &other) const {
      return name == other.name && arg_types == other.arg_types;
    }

    string GetName();

    // Attributes
    string         name;
    vector<string> arg_types;
  };

  //! Class to describe a substrait function extension (a URI for substrait function info)
  struct SubstraitFunctionExtensions {

    // Constructors
    SubstraitFunctionExtensions() = default;
    SubstraitFunctionExtensions(SubstraitCustomFunction function_p, string extension_path_p)
      : function(std::move(function_p)), extension_path(std::move(extension_path_p)) {}

    // Functions
    string GetExtensionURI() const;
    bool   IsNative() const;

    // Attributes
    SubstraitCustomFunction function;
    string                  extension_path;
  };

  //! Class that acts as a registry for substrait functions and their extensions
  struct SubstraitCustomFunctions {
    // type aliases for convenience
    using SubstraitTypeVec  = vector<::substrait::Type>;

    using SubstraitFnMap    = std::unordered_map< SubstraitCustomFunction
                                                 ,SubstraitFunctionExtensions
                                                 ,HashSubstraitFunctions     >;

    using SubstraitAnyFnMap = std::unordered_map< SubstraitCustomFunction
                                                 ,SubstraitFunctionExtensions
                                                 ,HashSubstraitFunctionsName>;

    // Constructors
    SubstraitCustomFunctions() = default;

    SubstraitCustomFunctions(SubstraitCustomFunctions& other)
      :  custom_functions(other.custom_functions)
        ,any_arg_functions(other.any_arg_functions)
        ,many_arg_functions(other.many_arg_functions) {}

    SubstraitCustomFunctions(SubstraitCustomFunctions&& other)
      :  custom_functions(std::move(other.custom_functions))
        ,any_arg_functions(std::move(other.any_arg_functions))
        ,many_arg_functions(std::move(other.many_arg_functions)) {}

    // Methods
    SubstraitFunctionExtensions Get(const string& name, const SubstraitTypeVec& types) const;

    // Static methods
	  static vector<string> GetTypes(const vector<substrait::Type>& types);

    static SubstraitCustomFunctions InitializedForDuckDB();

    private:
      // Private attributes
      SubstraitFnMap    custom_functions;   // For regular functions
      SubstraitAnyFnMap any_arg_functions;  // For "*"     functions
      SubstraitFnMap    many_arg_functions; // For "?"     functions

      // Private methods
      void InsertCustomFunction(string name_p, vector<string> types_p, string file_path);
      void InsertAllFunctions( const vector<vector<string>>& all_types
                              ,vector<idx_t>&                indices
                              ,int                           depth
                              ,string&                       name_p
                              ,string&                       file_path);
  };

} // namespace duckdb
