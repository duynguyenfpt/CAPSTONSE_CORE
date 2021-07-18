package models;

import java.util.ArrayList;
import java.util.List;

public class ColumnProperty implements Comparable<ColumnProperty> {
    private String field;
    private String type;
    private Integer typeSize = -1;
    private Boolean isSigned;
    // must be sorted
    private String defaultValue = "NULL";
    private List<String> possibleValues = new ArrayList<>();

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getTypeSize() {
        return typeSize;
    }

    public void setTypeSize(Integer typeSize) {
        this.typeSize = typeSize;
    }

    public boolean isSigned() {
        return isSigned;
    }

    public void setSigned(Boolean signed) {
        isSigned = signed;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public List<String> getPossibleValues() {
        return possibleValues;
    }

    public void setPossibleValues(List<String> possibleValues) {
        this.possibleValues = possibleValues;
        this.possibleValues.sort(null);
    }

    public ColumnProperty(String field, String type, int typeSize, boolean isSigned, String defaultValue, List<String> possibleValues) {
        this.field = field;
        this.type = type;
        this.typeSize = typeSize;
        this.isSigned = isSigned;
        this.defaultValue = defaultValue;
        this.possibleValues = possibleValues;
        this.possibleValues.sort(null);
    }

    public ColumnProperty() {
    }

    @Override
    public String toString() {
        return "ColumnProperty{" +
                "field='" + field + '\'' +
                ", type='" + type + '\'' +
                ", typeSize=" + typeSize +
                ", isSigned=" + isSigned +
                ", defaultValue='" + defaultValue + '\'' +
                ", possibleValues=" + possibleValues +
                '}';
    }

    @Override
    public int compareTo(ColumnProperty o) {
        return this.getField().compareToIgnoreCase(o.getField());
    }
}
