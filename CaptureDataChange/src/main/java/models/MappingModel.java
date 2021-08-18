package models;

import java.util.ArrayList;
import java.util.List;

public class MappingModel {
    private String colName;
    private ArrayList<String> listCol;

    public MappingModel(String colName, ArrayList<String> listCol) {
        this.colName = colName;
        this.listCol = listCol;
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
}
