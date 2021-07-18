package models.Alert;

import models.DDLModel;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

public class AlertModel {
    private String msg;
    private List<AlertOwner> owner;
    private String created;
    private String updated;
    private DDLModel metrics;
    private String entity_name;
    private String entity_type;
    private String alert_level;
    private String alert_cycle;
    private String process_name;

    public AlertModel(String msg, List<AlertOwner> owner, LocalDateTime created, LocalDateTime updated, DDLModel metrics, String entity_name, String entity_type, String alert_level, String alert_cycle, String process_name) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(ZoneId.systemDefault());;
        this.msg = msg;
        this.owner = owner;
        this.created = dtf.format(created);
        this.updated = dtf.format(updated);
        this.metrics = metrics;
        this.entity_name = entity_name;
        this.entity_type = entity_type;
        this.alert_level = alert_level;
        this.alert_cycle = alert_cycle;
        this.process_name = process_name;
    }

    public AlertModel() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(ZoneId.systemDefault());;
        created = dtf.format(LocalDateTime.now());
        updated = dtf.format(LocalDateTime.now());
        alert_level = "WARNING";
        alert_cycle = "REALTIME";
        process_name = "job_duynt_ok";
        AlertOwner ao = new AlertOwner("duynt142", "0914912929");
        owner = Arrays.asList(ao);
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public List<AlertOwner> getOwner() {
        return owner;
    }

    public void setOwner(List<AlertOwner> owner) {
        this.owner = owner;
    }

    public String getCreated() {
        return created;
    }

    public void setCreated(LocalDateTime created) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(ZoneId.systemDefault());
        this.created = dtf.format(created);
    }

    public String getUpdated() {
        return updated;
    }

    public void setUpdated(LocalDateTime updated) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(ZoneId.systemDefault());
        this.updated = dtf.format(updated);
    }

    public DDLModel getMetrics() {
        return metrics;
    }

    public void setMetrics(DDLModel metrics) {
        this.metrics = metrics;
    }

    public String getEntity_name() {
        return entity_name;
    }

    public void setEntity_name(String entity_name) {
        this.entity_name = entity_name;
    }

    public String getEntity_type() {
        return entity_type;
    }

    public void setEntity_type(String entity_type) {
        this.entity_type = entity_type;
    }

    public String getAlert_level() {
        return alert_level;
    }

    public void setAlert_level(String alert_level) {
        this.alert_level = alert_level;
    }

    public String getAlert_cycle() {
        return alert_cycle;
    }

    public void setAlert_cycle(String alert_cycle) {
        this.alert_cycle = alert_cycle;
    }

    public String getProcess_name() {
        return process_name;
    }

    public void setProcess_name(String process_name) {
        this.process_name = process_name;
    }
}
