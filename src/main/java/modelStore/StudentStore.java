package modelStore;

import Database.Connect;
import model.Student;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.jetbrains.annotations.Nullable;

import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class StudentStore extends CacheStoreAdapter<AffinityKey<Integer>, Student> {
    @Override
    public void loadCache(IgniteBiInClosure<AffinityKey<Integer>, Student> clo, @Nullable Object... args) {
        String sql="SELECT * FROM Student";
        try(Connect con=new Connect()){
            try(Statement stmt=con.getCon().createStatement()){
                try(ResultSet rs=stmt.executeQuery(sql)){
                    while(rs.next()){
                        Student student=new Student(
                                rs.getInt(1),    //studentID
                                rs.getString(2),    //studentName
                                rs.getInt(3),   //schoolID
                                rs.getString(4),    //sex
                                rs.getInt(5)    //classID
                        );
                        clo.apply(student.getKey(),student);
                    }
                }
            }catch (SQLException e){
                throw new CacheLoaderException("Fail to load students from cache store.",e);
            }
        }
    }

    @Override
    public Student load(AffinityKey<Integer> integer) throws CacheLoaderException {
        String sql="SELECT * FROM Student WHERE studentID=?";
        Student student=null;
        try(Connect con=new Connect()){
            try(PreparedStatement pstmt=con.getCon().prepareStatement(sql)){
                pstmt.setInt(1,integer.key());
                try(ResultSet rs=pstmt.executeQuery()){
                    if(rs.next()){
                        student=new Student(
                                rs.getInt(1),    //studentID
                                rs.getString(2),    //studentName
                                rs.getInt(3),   //schoolID
                                rs.getString(4),    //sex
                                rs.getInt(5)    //classID
                        );
                    }else
                        System.out.println("Student not found");
                }
            }catch (SQLException e){
                throw new CacheLoaderException("Fail to load student with studentID:    "+integer,e);
            }
        }
        return student;
    }

    @Override
    public Map<AffinityKey<Integer>, Student> loadAll(Iterable<? extends AffinityKey<Integer>> keys) {
        String sql="SELECT * FROM Student WHERE studentID=?";
        HashMap<AffinityKey<Integer>,Student> loaded=new HashMap<>();
        try(Connect con=new Connect()){
            try(PreparedStatement pstmt=con.getCon().prepareStatement(sql)){
                for(AffinityKey<Integer> key:keys){
                    pstmt.setInt(1,key.key());
                    try(ResultSet rs=pstmt.executeQuery()){
                        if(rs.next()){
                            Student student=new Student(
                                    rs.getInt(1),    //studentID
                                    rs.getString(2),    //studentName
                                    rs.getInt(3),   //schoolID
                                    rs.getString(4),    //sex
                                    rs.getInt(5)    //classID
                            );
                            loaded.put(key,student);
                        }
                    }
                }
            }catch (SQLException e){
                throw new CacheLoaderException("Fail to loadAll:    "+keys,e);
            }
        }
        if(loaded.size()==0)
            return null;
        else
            return loaded;
    }

    @Override
    public void write(Cache.Entry<? extends AffinityKey<Integer>, ? extends Student> entry) throws CacheWriterException {
        //PostgreSQL special statement
        //if student already exists,update its info,else,insert it into database
        String sql="INSERT INTO (studentID,studentName,schoolID,sex,classID)" +
                "VALUES (?,?,?,?,?) ON CONFLICT(studentID) " +
                "DO UPDATE SET studentID=?,studentName=?,schoolID=?,sex=?,classID=?";
        Student student=entry.getValue();
        try(Connect con=new Connect()){
            try(PreparedStatement pstmt=con.getCon().prepareStatement(sql)){
                //insert
                pstmt.setInt(1,student.getStudentID());
                pstmt.setString(2,student.getStudentName());
                pstmt.setInt(3,student.getSchoolID());
                pstmt.setString(4,student.getSex());
                pstmt.setInt(5,student.getClassID());

                //update
                pstmt.setInt(6,student.getStudentID());
                pstmt.setString(7,student.getStudentName());
                pstmt.setInt(8,student.getSchoolID());
                pstmt.setString(9,student.getSex());
                pstmt.setInt(10,student.getClassID());

                if(pstmt.executeUpdate()==0)
                    System.out.printf("Student not found");
            }catch (SQLException e){
                throw new CacheWriterException("Fail to update/insert student with studentID:   "+entry.getKey());
            }
        }
    }

    @Override
    public void writeAll(Collection<Cache.Entry<? extends AffinityKey<Integer>, ? extends Student>> entries) {
        //PostgreSQL special statement
        //if student already exists,update its info,else,insert it into database
        String sql="INSERT INTO Student (studentID,studentName,schoolID,sex,classID)" +
                "VALUES (?,?,?,?,?) ON CONFLICT(studentID) " +
                "DO UPDATE SET studentID=?,studentName=?,schoolID=?,sex=?,classID=?";
        try(Connect con=new Connect()){
            try(PreparedStatement pstmt=con.getCon().prepareStatement(sql)){
               for(Cache.Entry<? extends AffinityKey<Integer>, ? extends Student> entry:entries){
                   Student student=entry.getValue();

                   //insert
                   pstmt.setInt(1,student.getStudentID());
                   pstmt.setString(2,student.getStudentName());
                   pstmt.setInt(3,student.getSchoolID());
                   pstmt.setString(4,student.getSex());
                   pstmt.setInt(5,student.getClassID());

                   //update
                   pstmt.setInt(6,student.getStudentID());
                   pstmt.setString(7,student.getStudentName());
                   pstmt.setInt(8,student.getSchoolID());
                   pstmt.setString(9,student.getSex());
                   pstmt.setInt(10,student.getClassID());

                   pstmt.addBatch();
               }
               pstmt.executeBatch();
            }catch (SQLException e){
                throw new CacheLoaderException("Fail to writeAll:   "+entries,e);
            }
        }
    }

    @Override
    public void delete(Object o) throws CacheWriterException {
        String sql="DELETE FROM Student WHERE studentID=?";
        try(Connect con=new Connect()){
            try(PreparedStatement pstmt=con.getCon().prepareStatement(sql)){
                pstmt.setInt(1,(Integer)o);
                if(pstmt.executeUpdate()==0)
                    System.out.printf("Student not found");
            }catch (SQLException e){
                throw new CacheWriterException("Fail to delete student:  "+o,e);
            }
        }
    }

    @Override
    public void deleteAll(Collection<?> keys) {
        String sql="DELETE FROM Student WHERE studentID=?";
        try(Connect con=new Connect()){
            try(PreparedStatement pstmt=con.getCon().prepareStatement(sql)){
                for(Object o:keys){
                    pstmt.setInt(1,(Integer)o);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }catch (SQLException e){
                throw new CacheWriterException("Fail to deleteAll:  "+keys,e);
            }
        }
    }
}