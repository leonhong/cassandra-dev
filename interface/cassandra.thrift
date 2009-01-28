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
  list<column_t>	get_slice(string tablename,string key,string columnFamily_column, i32 start = -1 , i32 count = -1) throws (1: InvalidRequestException ire),

  column_t       	get_column(string tablename,string key,string columnFamily_column) throws (1: InvalidRequestException ire, 2: NotFoundException nfe),

  i32            	get_column_count(string tablename,string key,string columnFamily_column) throws (1: InvalidRequestException ire),

  async void     	insert(string tablename,string key,string columnFamily_column, string cellData, i64 timestamp),

  bool     		insert_blocking(string tablename, string key, string columnFamily_column, string cellData, i64 timestamp),

  async void     	batch_insert(batch_mutation_t batchMutation),

  bool           	batch_insert_blocking(batch_mutation_t batchMutation),

  async void     	remove(string tablename,string key,string columnFamily_column,i64 timestamp),

  list<superColumn_t> 	get_slice_super(string tablename, string key, string columnFamily_superColumnName, i32 start = -1 , i32 count = -1) throws (1: InvalidRequestException ire),

  superColumn_t       	get_superColumn(string tablename,string key,string columnFamily_superColumnName) throws (1: InvalidRequestException ire, 2: NotFoundException nfe),

  async void          	batch_insert_superColumn(batch_mutation_super_t batchMutationSuper),

  bool                	batch_insert_superColumn_blocking(batch_mutation_super_t batchMutationSuper),
  list<string>	        get_range(string tablename, string startkey) throws (1: InvalidRequestException ire),
}
