package io.wizzie.normalizer.base.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConversionUtils {
    private static final Logger log = LoggerFactory.getLogger(ConversionUtils.class);

    public static Long toLong(Object l) {
        Long result = null;

        try {
            if (l != null) {
                if (l instanceof Integer) {
                    result = ((Integer) l).longValue();
                } else if (l instanceof Long) {
                    result = (Long) l;
                } else if (l instanceof String) {
                    result = Long.valueOf((String) l);
                } else if (l instanceof Double) {
                    result = ((Double)l).longValue();
                } else if (l instanceof Float) {
                    result = ((Float)l).longValue();
                }
            }
        } catch (NumberFormatException ex) {
            log.error(ex.getMessage(), ex);
        }

        return result;
    }
}
