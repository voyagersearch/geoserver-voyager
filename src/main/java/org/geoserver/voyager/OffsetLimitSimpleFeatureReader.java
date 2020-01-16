package org.geoserver.voyager;

import org.geotools.data.simple.SimpleFeatureReader;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.util.NoSuchElementException;

public class OffsetLimitSimpleFeatureReader implements SimpleFeatureReader {

    final SimpleFeatureReader reader;
    final Integer offset;
    final Integer limit;

    int count = -1;

    public OffsetLimitSimpleFeatureReader(SimpleFeatureReader reader, Integer offset, Integer limit) {
        this.reader = reader;
        this.offset = offset;
        this.limit = limit;
    }

    @Override
    public SimpleFeatureType getFeatureType() {
        return reader.getFeatureType();
    }

    @Override
    public SimpleFeature next() throws IOException, IllegalArgumentException, NoSuchElementException {
        SimpleFeature f = reader.next();
        count++;
        return f;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (count == -1) {
            // initialize
            if (offset != null) {
                int i = 0;
                while (i++ < offset) {
                    if (reader.hasNext()) reader.next();
                    else break;
                }
            }
            count = 0;
        }

        if (count > limit) {
            return false;
        }

        return reader.hasNext();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
