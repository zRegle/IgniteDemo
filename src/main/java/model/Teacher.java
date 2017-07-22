package model;

import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;

public class Teacher implements Serializable {
    @QuerySqlField(index = true)
    private int teacherID;

    @QuerySqlField
    private String teacherName;

    @QuerySqlField(index = true)
    private int schoolID;

    @QuerySqlField
    private String title;

    @QuerySqlField(index = true)
    private int classID;

    private transient AffinityKey<Integer> key;


    public Teacher(int teacherID, String teacherName, int schoolID, String title, int classID) {
        this.teacherID = teacherID;
        this.teacherName = teacherName;
        this.schoolID = schoolID;
        this.title = title;
        this.classID = classID;
    }

    public int getTeacherID() {
        return teacherID;
    }

    public void setTeacherID(int teacherID) {
        this.teacherID = teacherID;
    }

    public String getTeacherName() {
        return teacherName;
    }

    public void setTeacherName(String teacherName) {
        this.teacherName = teacherName;
    }

    public int getSchoolID() {
        return schoolID;
    }

    public void setSchoolID(int schoolID) {
        this.schoolID = schoolID;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public int getClassID() {
        return classID;
    }

    public void setClassID(int classID) {
        this.classID = classID;
    }

    public AffinityKey<Integer> getKey() {
        if(key==null)
            key=new AffinityKey<>(teacherID,schoolID);
        return key;
    }

    @Override
    public String toString() {
        return "Teacher{" +
                "teacherID=" + teacherID +
                ", teacherName='" + teacherName + '\'' +
                ", schoolID=" + schoolID +
                ", title='" + title + '\'' +
                ", classID=" + classID +
                '}';
    }
}
