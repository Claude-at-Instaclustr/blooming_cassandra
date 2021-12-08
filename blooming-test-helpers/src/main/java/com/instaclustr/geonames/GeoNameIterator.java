package com.instaclustr.geonames;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.commons.lang3.time.StopWatch;

public class GeoNameIterator implements Iterator<GeoName>, AutoCloseable {

    public final static URL DEFAULT_INPUT = GeoNameIterator.class.getResource("/allCountries.txt");

    private final BufferedReader bufferedReader;
    private GeoName next;
    private int count = 0;
    private StopWatch stopwatch;
    private int limit = Integer.MAX_VALUE;

    public GeoNameIterator(URL inputFile) throws IOException {
        this(inputFile.openStream());
    }

    public GeoNameIterator(InputStream stream) {
        this(new InputStreamReader(stream));
    }

    public GeoNameIterator(Reader reader) {
        if (reader instanceof BufferedReader) {
            bufferedReader = (BufferedReader) reader;
        } else {
            bufferedReader = new BufferedReader(reader);
        }
        next = null;
    }

    public void setLimit( int limit ) {
        this.limit = limit;
    }

    @Override
    public void close() throws IOException {
        bufferedReader.close();
        stopwatch.stop();
    }

    @Override
    public boolean hasNext() {
        if (stopwatch == null) {
            stopwatch = new StopWatch();
            stopwatch.start();
        }
        if (next == null) {
            if (count >= limit) {
                return false;
            }
            String s;
            try {
                s = bufferedReader.readLine();
            } catch (IOException e) {
                return false;
            }
            if (s == null) {
                return false;
            }
            next = GeoName.Serde.deserialize(s);
        }
        return true;
    }

    @Override
    public GeoName next() {
        if (hasNext()) {
            try {
                count++;
                if ((count % 1000) == 0) {
                    System.out.println(String.format("read : %8d in %s", count, stopwatch.formatTime()));
                }
                return next;
            } finally {
                next = null;
            }
        } else {
            throw new NoSuchElementException();
        }
    }
}
