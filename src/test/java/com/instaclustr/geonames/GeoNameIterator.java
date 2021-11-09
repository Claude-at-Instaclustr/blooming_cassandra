package com.instaclustr.geonames;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class GeoNameIterator implements Iterator<GeoName>, AutoCloseable {

    public final static URL DEFAULT_INPUT = GeoNameIterator.class.getResource("/allCountries.txt");

    private final BufferedReader bufferedReader;
    private GeoName next;
    private int count = 0;

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

    @Override
    public void close() throws IOException {
        bufferedReader.close();
    }

    @Override
    public boolean hasNext() {
        if (next == null) {
            String s;
            try {
                s = bufferedReader.readLine();
            } catch (IOException e) {
                return false;
            }
            if (s == null)
            {
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
                    System.out.println( String.format( "read : %8d", count));
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
