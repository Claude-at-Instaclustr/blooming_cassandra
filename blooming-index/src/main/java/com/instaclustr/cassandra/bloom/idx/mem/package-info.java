package com.instaclustr.cassandra.bloom.idx.mem;
/**
    This version of the index uses and in memory map of the data on disk.
    It uses a memory mapped file to reference the complete index for operation.

    The strategy uses a couple of tables:

    Basetable key to internal id mapping.
    basetable-key : id

    id : basetable key mapping
    id : basetable-key

    It also uses a memory mapped file



 */