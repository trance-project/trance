TpchQueryTests.scala contains the compiler benchmark test queries from 

trance/compiler/src/main/scala/framework/examples
/tpch/ 

In TpchQueryTests there are 3 sections encapsulated in describe blocks.
These are FlatToNested, NestedToFlat & NestedToNested. Each test case contained in an it block corresponds to a test from it's section in the directory above.

Example: 
FlatToNested, Test1 corresponds to the query defined here: https://github.com/trance-project/trance/blob/trance_library/compiler/src/main/scala/framework/examples/tpch/FlatToNested.scala#L33

Using spark it's possible to run the test cases in TpchQueryTests to verify the library is behaving as expected based on the results.

GeneratedCodeTests.scala contains tests correspoding to the compiler benchmark tests also. The code in these tests is generated from the compiler. It's possible to run these with spark for further cross checking with the libary tests above.
