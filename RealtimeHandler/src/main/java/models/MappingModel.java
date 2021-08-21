package models;

import java.util.ArrayList;

public class MappingModel {
    private String colName;
    private ArrayList<String> listCol;
    private int is_unique;

    public MappingModel(String colName, ArrayList<String> listCol, int is_unique) {
        this.colName = colName;
        this.listCol = listCol;
        this.is_unique = is_unique;
    }

    public MappingModel() {
    }

    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }

    public ArrayList<String> getListCol() {
        return listCol;
    }

    public void setListCol(ArrayList<String> listCol) {
        this.listCol = listCol;
    }

    public int getIs_unique() {
        return is_unique;
    }

    public void setIs_unique(int is_unique) {
        this.is_unique = is_unique;
    }
}
