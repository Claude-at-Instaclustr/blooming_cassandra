package com.instaclustr.geonames;

import java.io.IOException;

import org.apache.commons.collections4.bloomfilter.BloomFilter;

public class Demo {

    public Demo() {
        // TODO Auto-generated constructor stub
    }

    public static void main(String[] args) throws IOException {
        GeoNameIterator iter = new GeoNameIterator( GeoNameIterator.DEFAULT_INPUT );
        int total = 0;
        int max = 0;
        int min = Integer.MAX_VALUE;
        int limit = 10;
        int[] counts = new int[256];
                ;
        for (int i=0;i<limit;i++) {
            int count = 0;
            GeoName gn = iter.next();
            System.out.println( "Cardinality "+gn.filter.cardinality() );
            byte[] buff = extractBytes( gn.filter );
            for (int j=0;j<buff.length;j++) {
                if (buff[j] != (byte)0) {
                    count++;
                    int val= 0xFF & buff[j];
                    counts[ val ]++;
                }
            }
            System.out.println( String.format( "%d: %d", i, count));
            total+=count;
            max = max<count?count:max;
            min = min>count?count:min;
        }
        double avg = (1.0*total)/limit;
        System.out.println( String.format( "Tot: %s Avg: %s Min: %s Max %s", total, avg, min, max));
//        total = 0;
//        for(int i=0;i<counts.length;i++)
//        {
//            System.out.println( String.format("count %s %s", i, counts[i] ) );
//            total += counts[i];
//
//        }
//        System.out.println( String.format("count total %s", total ) );
        iter.close();
    }

    private static byte[] extractBytes(BloomFilter filter) {

        long[] buff = BloomFilter.asBitMapArray(filter);
        byte[] codes = new byte[buff.length*Long.BYTES];
        int pos=0;
        for (long word : buff) {
            for (int i = 0; i < Long.BYTES; i++) {
                byte code = (byte) (word & 0xFF);
                word = word >> Byte.SIZE;
            codes[pos++] = code;
            }
        }
        return codes;

    }
}
