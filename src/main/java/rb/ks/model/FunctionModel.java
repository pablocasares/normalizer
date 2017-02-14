package rb.ks.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class FunctionModel {
    String className;
    Map<String, Object> properties;
    List<String> stores;
    String name;

    @JsonCreator
    public FunctionModel(@JsonProperty("name") String name,
                         @JsonProperty("className") String className,
                         @JsonProperty("properties") Map<String, Object> properties,
                         @JsonProperty("stores") List<String> stores) {
        this.name = name;
        this.className = className;
        this.properties = properties;
        this.stores = stores;
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public String getClassName() {
        return className;
    }

    @JsonProperty
    public Map<String, Object> getProperties() {
        return properties;
    }

    @JsonProperty
    public List<String> getStores() {
        return stores;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{")
                .append("className: ").append(className).append(", ")
                .append("properties: ").append(properties).append(", ")
                .append("stores: ").append(stores)
                .append("}");

        return builder.toString();
    }
}