package io.wizzie.normalizer.funcs.impl;

import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.funcs.MapperFunction;
import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parsii.eval.Expression;
import parsii.eval.Parser;
import parsii.eval.Scope;
import parsii.eval.Variable;
import parsii.tokenizer.ParseException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class ArithmeticMapper extends MapperFunction {

    List<Map<String, Object>> equations;
    Map<String, Variable> variableMap = new HashMap<>();
    Map<String, Expression> equationMap = new HashMap<>();
    Scope scope = Scope.create();
    private static final Logger log = LoggerFactory.getLogger(ArithmeticMapper.class);

    @Override
    public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {

        equations = (List<Map<String, Object>>) checkNotNull(properties.get("equations"), "equations cannot be null");
        for (Map<String, Object> equation : equations) {
            checkNotNull(equation.get("as"), "as cannot be null");
            checkNotNull(equation.get("equation"), "equation cannot be null");
            checkNotNull(equation.get("dimensions"), "dimensions cannot be null");
        }

        for (Map<String, Object> equation : equations) {
            List<String> dimensions = (List<String>) equation.get("dimensions");
            for (String dimension : dimensions) {
                if (!variableMap.containsKey(dimension)) {
                    Variable var = scope.getVariable(dimension);
                    variableMap.put(dimension, var);
                }
            }
            try {
                Expression expr = Parser.parse((String) equation.get("equation"), scope);
                equationMap.put((String) equation.get("equation"), expr);
            } catch (ParseException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    @Override
    public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {

        if (value != null) {

            for (Map<String, Object> equation : equations) {
                boolean everyVarExist = true;
                for (String dimension : ((List<String>) equation.get("dimensions"))) {
                    Object value2Set = value.get(dimension);
                    if (value2Set != null) {
                        try {
                            Number number = (Number) value2Set;
                            variableMap.get(dimension).setValue(number.doubleValue());
                        } catch (Exception e) {
                            everyVarExist = false;
                            log.debug(e.getMessage(), e);
                        }
                    } else {
                        everyVarExist = false;
                        log.debug("Dimension: " + dimension + " doesn't exist to evaluate the arithmetic expression.");
                        break;
                    }
                }

                if (everyVarExist) {
                    value.put((String) equation.get("as"), equationMap.get(equation.get("equation").toString()).evaluate());
                }
            }

            return new KeyValue<>(key, value);
        } else {
            return new KeyValue<>(key, null);
        }
    }


    public List<Map<String, Object>> getEquations() {
        return equations;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        builder.append("equations: ");
        builder.append(equations);
        builder.append("}");

        return builder.toString();
    }

    @Override
    public void stop() {

    }
}
