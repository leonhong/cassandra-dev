#
# Interface definition for Cassandra
# 

include "fb303.thrift"

namespace java com.facebook.infrastructure.service
namespace py com.facebook.infrastructure.service
namespace cpp com.facebook.infrastructure.service
namespace php com.facebook.infrastructure.service


struct column_t 
{
   1: string                        columnName,
   2: string                        value,
   3: i64                           timestamp,
}

typedef map< string, list<column_t>  > column_family_map

struct batch_mutation_t 
{
   1: string                        table,
   2: string                        key,
   3: column_family_map             cfmap,
}

struct superColumn_t 
{
   1: string                        name,
   2: list<column_t>				columns,
}

typedef map< string, list<superColumn_t>  > superColumn_family_map

struct batch_mutation_super_t {
   1: string                        table,
   2: string                        key,
   3: superColumn_family_map        cfmap,
}


exception InvalidRequestException {
    1: string why
}

exception NotFoundException {
}


service Cassandra extends fb303.FacebookService 
{
  # queries
  i32            	get_column_count(string tablename,string key,string columnFamily_column) throws (1: InvalidRequestException ire),
  column_t       	get_column(string tablename,string key,string columnFamily_column) throws (1: InvalidRequestException ire, 2: NotFoundException nfe),
  superColumn_t       	get_superColumn(string tablename,string key,string columnFamily_superColumnName) throws (1: InvalidRequestException ire, 2: NotFoundException nfe),
  list<column_t>	get_slice(string tablename,string key,string columnFamily_column, i32 start=-1 , i32 count=-1) throws (1: InvalidRequestException ire),
  list<superColumn_t> 	get_slice_super(string tablename, string key, string columnFamily_superColumnName, i32 start=-1 , i32 count=-1) throws (1: InvalidRequestException ire),

  # range query: returns matching keys
  list<string>	        get_range(string tablename, string startkey) throws (1: InvalidRequestException ire),

  # creating/updating data.  the return value will always be True
  # when block_for is not positive.
  bool     	insert(string tablename,string key,string columnFamily_column, string cellData, i64 timestamp, i32 block_for=0),
  bool         	batch_insert(batch_mutation_t batchMutation, i32 block_for=0),
  bool         	batch_insert_superColumn(batch_mutation_super_t batchMutationSuper, i32 block_for=0),

  # removing
  bool     	remove(string tablename, string key, string columnFamily_column, i64 timestamp, i32 block_for=0),
}
