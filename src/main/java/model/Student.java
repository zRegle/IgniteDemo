package model;

import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;

public class Student implements Serializable {
    @QuerySqlField(index = true)
    private int studentID;

    @QuerySqlField
    private String studentName;

    @QuerySqlField(index = true)
    private int schoolID;

    @QuerySqlField
    private String sex;

    @QuerySqlField(index = true)
    private int classID;

    private transient AffinityKey<Integer> key;


    public Student(int studentID, String studentName, int schoolID, String sex, int classID) {
        this.studentID = studentID;
        this.studentName = studentName;
        this.schoolID = schoolID;
        this.sex = sex;
        this.classID = classID;
    }


    public int getStudentID() {
        return studentID;
    }

    public void setStudentID(int studentID) {
        this.studentID = studentID;
    }

    public String getStudentName() {
        return studentName;
    }

    public void setStudentName(String studentName) {
        this.studentName = studentName;
    }

    public int getSchoolID() {
        return schoolID;
    }

    public void setSchoolID(int schoolID) {
        this.schoolID = schoolID;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public int getClassID() {
        return classID;
    }

    public void setClassID(int classID) {
        this.classID = classID;
    }

    public AffinityKey<Integer> getKey(){
        if(key==null)
            key=new AffinityKey<>(studentID,schoolID);
        return key;
    }

    @Override
    public String toString() {
        return "Student{" +
                "studentID=" + studentID +
                ", studentName='" + studentName + '\'' +
                ", schoolID=" + schoolID +
                ", sex='" + sex + '\'' +
                ", classID=" + classID +
                '}';
    }
}

