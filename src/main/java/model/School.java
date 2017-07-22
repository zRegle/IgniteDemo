package model;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;

public class School implements Serializable {
    @QuerySqlField(index = true)
    private int schoolID;

    @QuerySqlField
    private String schoolName;

    @QuerySqlField
    private String province;

    @QuerySqlField
    private String city;

    @QuerySqlField
    private String level;


    public School(int schoolID, String schoolName, String province, String city, String level) {
        this.schoolID = schoolID;
        this.schoolName = schoolName;
        this.province = province;
        this.city = city;
        this.level = level;
    }

    public int getSchoolID() {
        return schoolID;
    }

    public void setSchoolID(int schoolID) {
        this.schoolID = schoolID;
    }

    public String getSchoolName() {
        return schoolName;
    }

    public void setSchoolName(String schoolName) {
        this.schoolName = schoolName;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    @Override
    public String toString() {
        return "School{" +
                "schoolID=" + schoolID +
                ", schoolName='" + schoolName + '\'' +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", level='" + level + '\'' +
                '}';
    }
}
