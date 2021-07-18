package utils;

import models.ColumnProperty;
import models.DDLModel;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {
    public static List<List<String>> compareDiff(List<String> list1, List<String> list2) {
        Collections.sort(list1);
        Collections.sort(list2);
        int index1 = 0;
        int index2 = 0;
        ArrayList<String> diffTable = new ArrayList<>();
        ArrayList<String> commonTable = new ArrayList<>();
        while (index1 < list1.size() || index2 < list2.size()) {
            if (index1 == list1.size()) {
                while (index2 < list2.size()) {
                    diffTable.add(list2.get(index2));
                    index2++;
                }
                break;
            }
            if (index2 == list2.size()) {
                while (index1 < list1.size()) {
                    diffTable.add(list1.get(index1));
                    index1++;
                }
                break;
            }
            int compareVal = list1.get(index1).compareToIgnoreCase(list2.get(index2));
            if (compareVal < 0) {
                while (compareVal < 0 && index1 < list1.size()) {
                    diffTable.add(list1.get(index1));
                    index1++;
                    compareVal = list1.get(index1).compareToIgnoreCase(list2.get(index2));
                }
            } else if (compareVal > 0) {
                while (compareVal > 0 && index2 < list2.size()) {
                    diffTable.add(list2.get(index2));
                    index2++;
                    compareVal = list1.get(index1).compareToIgnoreCase(list2.get(index2));
                }
            } else if (compareVal == 0) {
                commonTable.add(list1.get(index1));
                index1++;
                index2++;
            }
        }
        return new ArrayList<>(Arrays.asList(commonTable, diffTable));
    }

    public static List<DDLModel> compareDiffColumn(List<ColumnProperty> listOld, List<ColumnProperty> listNew, String table, String dbType) {
        Collections.sort(listOld);
        Collections.sort(listNew);
        int index1 = 0;
        int index2 = 0;
        ArrayList<DDLModel> result = new ArrayList<>();
        while (index1 <= listOld.size() || index2 <= listNew.size()) {
            if (index1 == listOld.size()) {
                // if there is no element in listOld then take the rest listNew
                while (index2 < listNew.size()) {
                    index2 = insertExistInListNewOnly(listNew, table, dbType, index2, result);
                }
                break;
            }
            if (index2 == listNew.size()) {
                // if there is no element in listNew then take the rest listOld
                while (index1 < listOld.size()) {
                    index1 = insertExistListOldOnly(listOld, table, dbType, index1, result);
                }
                break;
            }
            int compareVal = listOld.get(index1).compareTo(listNew.get(index2));
            if (compareVal < 0) {
                while (compareVal < 0 && index1 < listOld.size()) {
                    // loop until find element that has field greater or equal to list new pointer
                    index1 = insertExistListOldOnly(listOld, table, dbType, index1, result);
                    if (index1 == listOld.size()) {
                        break;
                    }
                    compareVal = listOld.get(index1).compareTo(listNew.get(index2));
                }
            } else if (compareVal > 0) {
                while (compareVal > 0 && index2 < listNew.size()) {
                    // loop until find element that has field greater or equal to list old pointer
                    index2 = insertExistInListNewOnly(listNew, table, dbType, index2, result);
                    if (index2 == listNew.size()) {
                        break;
                    }
                    compareVal = listOld.get(index1).compareTo(listNew.get(index2));
                }
            } else if (compareVal == 0) {
                // fields are the same
                // check if type change
                if (!listNew.get(index2).getType().equals(listOld.get(index1).getType())) {
                    DDLModel model = new DDLModel(dbType, table);
                    model.setFieldChanged(listNew.get(index2).getField());
                    model.setChangeType("TYPE_CHANGED");
                    model.setNewValue(listNew.get(index2).getType());
                    model.setOldValue(listOld.get(index1).getType());
                    result.add(model);
                }
                if (!Objects.isNull(listNew.get(index2).getTypeSize()) ||
                        !Objects.isNull(listOld.get(index1).getTypeSize())) {
                    if (Objects.isNull(listNew.get(index2).getTypeSize())) {
                        DDLModel model = new DDLModel(dbType, table);
                        model.setFieldChanged(listNew.get(index2).getField());
                        model.setChangeType("TYPE_SIZE_CHANGED");
                        model.setNewValue(null);
                        model.setOldValue(listOld.get(index1).getTypeSize() + "");
                        result.add(model);
                    } else if (Objects.isNull(listOld.get(index1).getTypeSize())) {
                        DDLModel model = new DDLModel(dbType, table);
                        model.setFieldChanged(listNew.get(index2).getField());
                        model.setChangeType("TYPE_SIZE_CHANGED");
                        model.setNewValue(listNew.get(index2).getTypeSize() + "");
                        model.setOldValue(null);
                        result.add(model);
                    } else if (listNew.get(index2).getTypeSize() != listOld.get(index1).getTypeSize()) {
                        DDLModel model = new DDLModel(dbType, table);
                        model.setFieldChanged(listNew.get(index2).getField());
                        model.setChangeType("TYPE_SIZE_CHANGED");
                        model.setNewValue(listNew.get(index2).getTypeSize() + "");
                        model.setOldValue(listOld.get(index1).getTypeSize() + "");
                        result.add(model);
                    }
                }
                //
                if (!listNew.get(index2).getDefaultValue().equals(listOld.get(index1).getDefaultValue())) {
                    DDLModel model = new DDLModel(dbType, table);
                    model.setFieldChanged(listNew.get(index2).getField());
                    model.setChangeType("DEFAULT_VALUE_CHANGED");
                    model.setNewValue(listNew.get(index2).getDefaultValue());
                    model.setOldValue(listOld.get(index1).getDefaultValue());
                    result.add(model);
                }
                //
                //
                if (!listOld.get(index1).getPossibleValues().toString().equals(listNew.get(index2).getPossibleValues().toString())) {
                    DDLModel model = new DDLModel(dbType, table);
                    model.setFieldChanged(listNew.get(index2).getField());
                    model.setChangeType("POSSIBLE_VALUE_CHANGE");
                    model.setNewValue(listNew.get(index2).getPossibleValues().toString());
                    model.setOldValue(listOld.get(index1).getPossibleValues().toString());
                    result.add(model);
                }
                index1++;
                index2++;
            }
        }
        return result;
    }

    private static int insertExistInListNewOnly(List<ColumnProperty> listNew, String table, String dbType, int index2, ArrayList<DDLModel> result) {
        DDLModel model = new DDLModel(dbType, table);
        model.setFieldChanged(listNew.get(index2).getField());
        model.setChangeType("FIELD_CHANGED");
        model.setOldValue(null);
        model.setNewValue(listNew.get(index2).getField());
        result.add(model);
        index2++;
        return index2;
    }

    private static int insertExistListOldOnly(List<ColumnProperty> listOld, String table, String dbType, int index1, ArrayList<DDLModel> result) {
        DDLModel model = new DDLModel(dbType, table);
        model.setFieldChanged(listOld.get(index1).getField());
        model.setChangeType("FIELD_CHANGED");
        model.setOldValue(listOld.get(index1).getField());
        model.setNewValue(null);
        result.add(model);
        index1++;
        return index1;
    }

    public static String getKeyFromPath(String path) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(path));
        String line = null;
        try {
            line = br.readLine();
            return line;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            br.close();
            if (line == null) {
                throw new Exception("There is no key in file");
            }
        }
        return null;
    }

    public static String getHostFromConnection(Connection connection) throws SQLException {
        String url = connection.getMetaData().getURL();
        Pattern pattern = Pattern.compile("(\\d+[.:])+");
        Matcher matcher = pattern.matcher(url);
        while (matcher.find()) {
            return matcher.group().substring(0, matcher.group().length() - 1);
        }
        return null;
    }
}
