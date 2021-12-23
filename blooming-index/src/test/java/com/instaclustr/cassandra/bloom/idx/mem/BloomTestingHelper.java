package com.instaclustr.cassandra.bloom.idx.mem;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.commons.codec.digest.MurmurHash3;
import org.apache.commons.collections4.bloomfilter.BitMap;
import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.apache.commons.collections4.bloomfilter.hasher.Hasher;
import org.apache.commons.collections4.bloomfilter.hasher.HasherCollection;
import org.apache.commons.collections4.bloomfilter.hasher.SimpleHasher;

public class BloomTestingHelper {

    private BloomTestingHelper() {
        // TODO Auto-generated constructor stub
    }

    public static Hasher asHasher(String phrase) {
        HasherCollection result = new HasherCollection();
        for (String word : phrase.split(" ")) {
            long[] hash = MurmurHash3.hash128(word.getBytes(StandardCharsets.UTF_8));
            result.add(new SimpleHasher(hash[0], hash[1]));
        }
        return result;
    }

    public static ByteBuffer asByteBuffer(Hasher hasher, Shape shape) {
        ByteBuffer result = ByteBuffer.allocate(BitMap.numberOfBitMaps(shape.getNumberOfBits()) * Long.BYTES);
        BitMapProducer.fromIndexProducer(hasher.indices(shape), shape.getNumberOfBits()).forEachBitMap((l) -> {
            result.putLong(l);
            return true;
        });
        result.flip();
        return result;
    }

}
