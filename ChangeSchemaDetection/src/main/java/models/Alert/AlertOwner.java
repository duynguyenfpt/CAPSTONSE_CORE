package models.Alert;

public class AlertOwner {
    private String owner;
    private String phone;

    public AlertOwner(String owner, String phone) {
        this.owner = owner;
        this.phone = phone;
    }

    public AlertOwner() {
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }
}
