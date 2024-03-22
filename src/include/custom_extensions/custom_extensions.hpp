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

  //! Class to describe a custom, individual substrait function
  struct SubstraitCustomFunction {

    // Constructors
    SubstraitCustomFunction() = default;
    SubstraitCustomFunction(string name_p, vector<string> arg_types_p)
        : name(std::move(name_p)), arg_types(std::move(arg_types_p)) {}

    // Functions
    string GetName();
    bool operator==(const SubstraitCustomFunction &other) const {
      return name == other.name && arg_types == other.arg_types;
    }

    // Attributes
    string         name;
    vector<string> arg_types;
  };


  //! Class to describe a substrait function extension
  struct SubstraitFunctionExtensions {

    // Constructors
    SubstraitFunctionExtensions() = default;
    SubstraitFunctionExtensions( SubstraitCustomFunction function_p
                                ,string                  extension_path_p)
        :  function      (std::move(function_p))
          ,extension_path(std::move(extension_path_p)) {}

    // Functions
    bool   IsNative();
    string GetExtensionURI();

    // Attributes
    SubstraitCustomFunction function;
    string                  extension_path;
  };


  //! Hash function for a custom substrait function based on its signature.
  struct HashSubstraitFunctions {
    size_t operator()(SubstraitCustomFunction const &custom_function) const noexcept {
      auto& fn_argtypes = custom_function.arg_types;

      // Hash the type name for each function argument
      auto hashed_argtypes = Hash(fn_argtypes[0].c_str());
      for (idx_t arg_ndx = 1; arg_ndx < fn_argtypes.size(); arg_ndx++) {
        hashed_argtypes = CombineHash(
           hashed_argtypes
          ,Hash(fn_argtypes[arg_ndx].c_str())
        );
      }

      // Combine hashes for the function name and its argument types
      auto hashed_name = Hash(custom_function.name.c_str());
      return CombineHash(hashed_name, hashed_argtypes);
    }
  };


  //! Class that contains custom substrait functions and function extensions
  struct SubstraitCustomFunctions {
    // type aliases for convenience
    using SubstraitTypeVec = vector<::substrait::Type>;
    using SubstraitFnMap   = std::unordered_map< SubstraitCustomFunction
                                                ,SubstraitFunctionExtensions
                                                ,HashSubstraitFunctions     >;

    // Constructors
    SubstraitCustomFunctions();

    // Functions
    void Initialize();
    SubstraitFunctionExtensions Get( const string&           name
                                    ,const SubstraitTypeVec& types) const;

    private:
      SubstraitFnMap custom_functions;

      void InsertCustomFunction(string name_p, vector<string> types_p, string file_path);
  };

} // namespace duckdb
