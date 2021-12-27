package com.instaclustr.cassandra.bloom.idx.mem;
/**
 * This version of the index uses [Flat Bloofi](http://dx.doi.org/10.1016/j.is.2015.01.002) implemented with memory mapped files.
 *
 * The Flat Bloofi index is implemented using the BloomTable and BitTable implementations.
 *
 * Mapping between the Cassandra row and an arbitrary Flat Bloofi is performed via the IdxMap, BufferTable, and
 * BufferTableIdx classes.
 *
 * See README.md for more detail.
 *
  */