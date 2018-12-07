package io.wizzie.normalizer.funcs.impl;

import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.exceptions.FunctionException;
import io.wizzie.normalizer.funcs.FilterFunc;

import java.math.BigDecimal;
import java.sql.Time;
import java.text.NumberFormat;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NumberComparingFilter extends FilterFunc {
    private static final Logger log = LoggerFactory.getLogger(ArithmeticMapper.class);

    private final static String FIRST_COMPARABLE_DIMENSION = "firstComparableDimension";
    private final static String SECOND_COMPARABLE = "secondComparable";
    private final static String CONDITION = "condition";

    enum Condition {
        GREATER {
            public boolean compare(Number number1, Number number2) { return comp(number1, number2) == 1; }
        },
        LOWER {
            public boolean compare(Number number1, Number number2) { return comp(number1, number2) == -1; }
        };

        static int comp(Number number1, Number number2) {
            BigDecimal num1 = new BigDecimal(number1.doubleValue());
            BigDecimal num2 = new BigDecimal(number2.doubleValue());

            return num1.compareTo(num2);
        }

        public abstract boolean compare(Number number1, Number number2);
    }

    private Condition condition = Condition.GREATER;
    private String firstComparableDimension;
    private Object secondComparable;

    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
        String conditionValue = (String) properties.getOrDefault(CONDITION, "GREATER");
        condition = conditionValue == null ? Condition.GREATER : Condition.valueOf(conditionValue.toUpperCase());

        firstComparableDimension = (String) properties.get(FIRST_COMPARABLE_DIMENSION);

        if (firstComparableDimension == null) {
            throw new FunctionException("firstComparableDimension property cannot be null");
        }

        secondComparable = properties.get(SECOND_COMPARABLE);

        if (!(secondComparable instanceof String || secondComparable instanceof Number)) {
            throw new FunctionException("secondComparable property isn't a number or string or it's null");
        }
    }

    public Boolean process(String key, Map<String, Object> message) {
        if (message != null && message.containsKey(firstComparableDimension)) {
            Object dim1Value = message.get(firstComparableDimension);
            Object dim2Value;
            if (dim1Value instanceof Number) {
                if (secondComparable instanceof String && message.containsKey(secondComparable)) {
                    dim2Value = message.get(secondComparable);
                } else {
                    dim2Value = secondComparable;
                }

                return dim2Value instanceof Number && condition.compare((Number) dim1Value, (Number) dim2Value);
            } else {
                log.warn("Dimension <{}> with value <{}> isn't a number", firstComparableDimension, dim1Value);
            }
        }

        return false;
    }

    public void stop() {

    }
}
