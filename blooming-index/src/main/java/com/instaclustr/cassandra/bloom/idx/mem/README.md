 
# Layout

```
 [Cassandra Index ]       [ Idx Map   ]       [ Buffer Table Idx ]        [ Buffer Table   ]
 [--------------- ]       [-----------]       [------------------]        [----------------]
 [ Base table key ]   /-->[ (offset ) ]   /-->[ (offset)         ]   /--->[ (offset)       ]
 [ Bloofi Idx key ]---+   [-key idx---]---/   [ status flag byte ]   |    [ Base table key ]
 [----------------]   |   [-----------]       [ Key offset       ]---/    [----------------]
                      |                       [ Key length       ]
 [Busy Table     ]    |   [Bloom Table]       [ Key allocated    ]
 [---------------]    |   [-----------]       [------------------]
 [ Deleted bits  ]    \-->[ (offset)  ]
 [---------------]        [ Data      ]
                          [-----------]
 
 ```
 

## Cassandra Index
 
The Cassandra Index is a standard Cassandra index table and resolves the Base table key to the Bloom filter index.  The Bloofi index is used to calculate the offset into the Bloom table, Busy Table, and Idx Map Tables.
 
## Idx map
 
The Idx map is a fixed record length table that is the start of the process to convert the Bloofi Idx Key back to the Base table key.  it maps the idx value to an offset in the Buffer Table Idx.  This allows the base table key to change without significantly impacting the Bloom filter index.  

## Buffer Table Idx

The Buffer Table Idx is a fixed recrod length table that tracks the status in the Buffer table.  This tracks the location, allocated space, used space, and status of each block in the buffer table.

## Buffer Table

The Buffer Table is a variable length table that contains the Base table key.  Every record in the Buffer Table is associated with a Buffer Table Idx record that keeps track of its location and status.

## Busy Table

The Busy Table is part of the Flat Bloofi implementation.  It has one bit for each Bloom filter entry in the Bloom Table.  If the bit is enabled the Bloom Table entry is valid, other wise the Bloom Table entry is considered to be deleted.  This table is used to filter invalid entries on read and to locate deleted entries for reuse during insert.

## Bloom Table

The Flat Bloofi Bloom filter table.  This table has a complex structure that decomposes 64 Filters into a single Buffer block.  Details of how the mapping is performed are in the BloomTable documentation and in the original [Flat Bloofi](http://dx.doi.org/10.1016/j.is.2015.01.002) paper.

# Common operations

## Insert a new row

When a new row is inserted in the Base table the Busy Table is scanned for the first zero bit.  The position of that bit is the Bloom filter index.  If there is no zero bit a new block of 64 bits is added to the table and first zero bit position used as the Bloom filter index.

The Bloom filter is written into the Bloom table at the index location.

The Buffer Table Idx is scanned for a free buffer space that is big enough to contain the Base table key.  If none is found a new buffer the size of the key is added to the end of the Buffer table and a new record is added to the Buffer Table Idx that points to the new Buffer table entry.  The Base table key is written to the selected Buffer Table offset.  The associated Buffer Table Idx record is updated ot show that it is not deleted, and to specify the actual length of the Base table key.

The offset into the Idx Map table is calculated from the Bloom filter index and the Buffer Table Idx offset is written there.

Finally the Cassandra index is updated to contain the Base table key and the Bloom filter index.

## Delete a row

When a row is deleted the Base table key is looked up in the Cassandra index and the Bloom filter index returned.  The bit for the Bloom filter index in the Busy Table is turned off, effectively deleting the index.  The index Map is consulted and the associated Buffer Table Idx record updated to make the Buffer Table entry as deleted.

## Update a row

### Bloom filter update

When the Bloom filter in a row is updated the Cassandra index is consulted and the Bloom filter index retrieved.  The Bloom table is then updaed to reflect the new Bloom filter value.

### Key update

When the key for the Base table is updated the Cassandra index is consulted and the Bloom filter index retrieved. The Idx map is consulted and the Buffer Table idx located.  If there is space in the old Buffer table entry for the new key it is written over the old entry and Buffer Table Idx updated to reflect any length change.

if the new buffer does not fit in the space for the old buffer, The Buffer Table Idx is scanned for a record large enough to hold the buffer.  If none is found a new one is added.  The Buffer Table Idx is then properly populated, the Idx Map record updated to point to the new Buffer Table Idx and the original Buffer Table Idx marked as deleted.

Finally the original Cassandra index is deleted and the new Base table key and Bloom filter index written to the Cassandra index.

# Diagnostic Utilities

## FlatBloofi utility

The diagnostic utility found in the FlatBloofi class isa utility that dumps the Bloom filters and their status.

The options are:

 * -d or --directory : The directory with FlatBloofi files to process.
 * -h or --help : display the current help
 * -i or --index : A specific index to display/dump.  May be specified more than once.
 * -n or --bits : the number of bits in the Bloom filters
 * -o or --output : the name of the output file.  If not specified results will be printed to Standard out.

The output is a CSV file comprising:

 * 'index' : The Bloom filter id.
 * 'deleted' : True if the bloom filter is deleted (available for reuse)
 * 'filter' : The hex representation of the bloom filter if available.

## BufferTable utility

The diagnostic utility is found in the BufferTable class is a utility that dumps the mapping chains for the various tables.

The options are:

  * -b or --block-size: The size in bytes of the table blocks, The default for the FlatBloofi index implementation this is 8.
  * -h or --help: display the current help.
  * -i or --input: The name of the buffer table to process.
  * -o or --output: The name of the output buffer to write.  if not specified results will be printed to Standard out.

The output is a CSV file comprising:
 * 'index': The Bloom filter index.
 * 'initialized': True if the index map was initialized.  Any record not initialized will not be used.
 * 'reference': The Buffer Table Idx record the index map points to.
 * 'idx_init': True if the Buffer Table Idx was initialized, false otherwise.  Uninitialized reocrds will not be used.
 * 'available': True if the Buffer Table entry is deleted and not invalid.
 * 'deleted': True if the Buffer Table entry is deleted.
 * 'invalid': True if the Buffer Table entry is invalid.  Invalid entries are marked as such duing processing and should become available or not deleted.
 * 'used':  The number of bytes in the Buffer table that are used.
 * 'allocated': The number of bytes in the Buffer table that are allocated.
 * 'offset': The offset into the Buffer table for the entry
 * 'hex': The Cassandra base table key in Hex if the entry is valid.
 * 'char': The Cassandra base table key as a string if the entry is valid.
 
