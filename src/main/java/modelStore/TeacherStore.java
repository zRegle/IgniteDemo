package modelStore;

import Database.Connect;
import model.Teacher;
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

public class TeacherStore extends CacheStoreAdapter<AffinityKey<Integer>, Teacher> {
    @Override
    public void loadCache(IgniteBiInClosure<AffinityKey<Integer>, Teacher> clo, @Nullable Object... args) {
        String sql="SELECT * FROM Teacher";
        try(Connect con=new Connect()){
            try(Statement stmt=con.getCon().createStatement()){
                try(ResultSet rs=stmt.executeQuery(sql)){
                    while(rs.next()){
                        Teacher teacher=new Teacher(
                                rs.getInt(1),    //teacherID
                                rs.getString(2),    //teacherName
                                rs.getInt(3),   //schoolID
                                rs.getString(4),    //title
                                rs.getInt(5)    //classID
                        );
                        clo.apply(teacher.getKey(),teacher);
                    }
                }

            }catch (SQLException e){
                throw new CacheLoaderException("Fail to load teachers from cache store.",e);
            }
        }
    }

    @Override
    public Teacher load(AffinityKey<Integer> integerAffinityKey) throws CacheLoaderException {
        String sql="SELECT * FROM Teacher WHERE teacherID=?";
        Teacher teacher=null;
        try(Connect con=new Connect()){
            try(PreparedStatement pstmt=con.getCon().prepareStatement(sql)){
                pstmt.setInt(1,integerAffinityKey.key());
                try(ResultSet rs=pstmt.executeQuery()){
                    if(rs.next()){
                        teacher=new Teacher(
                                rs.getInt(1),    //teacherID
                                rs.getString(2),    //teacherName
                                rs.getInt(3),   //schoolID
                                rs.getString(4),    //title
                                rs.getInt(5)    //classID
                        );
                    }
                    else
                        System.out.println("Teacher not found");
                }
            }catch (SQLException e){
                throw new CacheLoaderException("Fail to load teacher with teacherID:    "+integerAffinityKey,e);
            }
        }
        return teacher;
    }

    @Override
    public Map<AffinityKey<Integer>, Teacher> loadAll(Iterable<? extends AffinityKey<Integer>> keys) {
        String sql="SELECT * FROM Teacher WHERE teacherID=?";
        HashMap<AffinityKey<Integer>,Teacher> loaded=new HashMap<>();
        try(Connect con=new Connect()){
            try(PreparedStatement pstmt=con.getCon().prepareStatement(sql)){
                for(AffinityKey<Integer> key:keys){
                    pstmt.setInt(1,key.key());
                    try(ResultSet rs=pstmt.executeQuery()){
                        if(rs.next()){
                            Teacher teacher=new Teacher(
                                    rs.getInt(1),    //teacherID
                                    rs.getString(2),    //teacherName
                                    rs.getInt(3),   //schoolID
                                    rs.getString(4),    //title
                                    rs.getInt(5)    //classID
                            );
                            loaded.put(key,teacher);
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
    public void write(Cache.Entry<? extends AffinityKey<Integer>, ? extends Teacher> entry) throws CacheWriterException {
        //PostgreSQL special statement
        //if teacher already exists,update its info,else,insert it into database
        String sql="INSERT INTO Teacher (teacherID,teacherName,schoolID,title,classID)" +
                "VALUES (?,?,?,?,?) ON CONFLICT(teacherID) " +
                "DO UPDATE SET teacherID=?,teacherName=?,schoolID=?,title=?,classID=?";
        Teacher teacher=entry.getValue();
        try(Connect con=new Connect()){
            try(PreparedStatement pstmt=con.getCon().prepareStatement(sql)){
                //insert
                pstmt.setInt(1,teacher.getTeacherID());
                pstmt.setString(2,teacher.getTeacherName());
                pstmt.setInt(3,teacher.getSchoolID());
                pstmt.setString(4,teacher.getTitle());
                pstmt.setInt(5,teacher.getClassID());

                //update
                pstmt.setInt(6,teacher.getTeacherID());
                pstmt.setString(7,teacher.getTeacherName());
                pstmt.setInt(8,teacher.getSchoolID());
                pstmt.setString(9,teacher.getTitle());
                pstmt.setInt(10,teacher.getClassID());

                if(pstmt.executeUpdate()==0)
                    System.out.printf("Teacher not found");
            }catch (SQLException e){
                throw new CacheWriterException("Fail to update/insert teacher with teacherID:   "+entry.getKey());
            }
        }
    }

    @Override
    public void writeAll(Collection<Cache.Entry<? extends AffinityKey<Integer>, ? extends Teacher>> entries) {
        //PostgreSQL special statement
        //if student already exists,update its info,else,insert it into database
        String sql="INSERT INTO Teacher (teacherID,teacherName,schoolID,title,classID)" +
                "VALUES (?,?,?,?,?) ON CONFLICT(teacherID) " +
                "DO UPDATE SET teacherID=?,teacherName=?,schoolID=?,title=?,classID=?";
        try(Connect con=new Connect()){
            try(PreparedStatement pstmt=con.getCon().prepareStatement(sql)){
                for(Cache.Entry<? extends AffinityKey<Integer>, ? extends Teacher> entry:entries){
                    Teacher teacher=entry.getValue();

                    //insert
                    pstmt.setInt(1,teacher.getTeacherID());
                    pstmt.setString(2,teacher.getTeacherName());
                    pstmt.setInt(3,teacher.getSchoolID());
                    pstmt.setString(4,teacher.getTitle());
                    pstmt.setInt(5,teacher.getClassID());

                    //update
                    pstmt.setInt(6,teacher.getTeacherID());
                    pstmt.setString(7,teacher.getTeacherName());
                    pstmt.setInt(8,teacher.getSchoolID());
                    pstmt.setString(9,teacher.getTitle());
                    pstmt.setInt(10,teacher.getClassID());

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
        String sql="DELETE FROM Teacher WHERE teacherID=?";
        try(Connect con=new Connect()){
            try(PreparedStatement pstmt=con.getCon().prepareStatement(sql)){
                pstmt.setInt(1,(Integer)o);
                if(pstmt.executeUpdate()==0)
                    System.out.printf("Teacher not found");
            }catch (SQLException e){
                throw new CacheWriterException("Fail to delete teacher:  "+o,e);
            }
        }
    }

    @Override
    public void deleteAll(Collection<?> keys) {
        String sql="DELETE FROM Teacher WHERE teacherID=?";
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