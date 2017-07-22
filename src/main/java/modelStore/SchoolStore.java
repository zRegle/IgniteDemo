package modelStore;

import Database.Connect;
import model.School;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.jetbrains.annotations.Nullable;

import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SchoolStore extends CacheStoreAdapter<Integer, School> {
    @Override
    public void loadCache(IgniteBiInClosure<Integer, School> clo, @Nullable Object... args) {
        String sql="";
        try(Connect con=new Connect()){
            try(PreparedStatement pstmt=con.getCon().prepareStatement(sql)){
                try(ResultSet rs=pstmt.executeQuery()){
                    while(rs.next()){
                        School school=new School(
                                rs.getInt(1),   //schoolID
                                rs.getString(2),    //schoolName
                                rs.getString(3),    //province
                                rs.getString(4),    //city
                                rs.getString(5) //level
                        );
                        clo.apply(school.getSchoolID(),school);
                    }
                }
            }catch (SQLException e){
                throw new CacheLoaderException("Fail to load schools from cache.",e);
            }
        }
    }

    @Override
    public School load(Integer integer) throws CacheLoaderException {
        String sql="SELECT * FROM School WHERE schoolID=?";
        School school=null;
        try(Connect con=new Connect()){
            try(PreparedStatement pstmt=con.getCon().prepareStatement(sql)){
                pstmt.setInt(1,integer);
                try(ResultSet rs=pstmt.executeQuery()){
                    if(rs.next()){
                        school=new School(
                                rs.getInt(1),   //schoolID
                                rs.getString(2),    //schoolName
                                rs.getString(3),    //province
                                rs.getString(4),    //city
                                rs.getString(5) //level
                        );
                    }
                    else
                        System.out.println("School not found");
                }
            }catch (SQLException e){
                throw new CacheLoaderException("Fail to load school with schoolID:    "+integer,e);
            }
        }
        return school;
    }

    @Override
    public Map<Integer, School> loadAll(Iterable<? extends Integer> keys) {
        String sql="SELECT * FROM School WHERE schoolID=?";
        HashMap<Integer,School> loaded=new HashMap<>();
        try(Connect con=new Connect()){
            try(PreparedStatement pstmt=con.getCon().prepareStatement(sql)){
                for(Integer key:keys){
                    pstmt.setInt(1,key);
                    try(ResultSet rs=pstmt.executeQuery()){
                        if(rs.next()){
                            School school=new School(
                                    rs.getInt(1),   //schoolID
                                    rs.getString(2),    //schoolName
                                    rs.getString(3),    //province
                                    rs.getString(4),    //city
                                    rs.getString(5) //level
                            );
                            loaded.put(key,school);
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
    public void write(Cache.Entry<? extends Integer, ? extends School> entry) throws CacheWriterException {
        //PostgreSQL special statement
        //if school already exists,update its info,else,insert it into database
        String sql="INSERT INTO School (schoolID,schoolName,province,city,level)" +
                "VALUES (?,?,?,?,?) ON CONFLICT(schoolID) " +
                "DO UPDATE SET schoolID=?,schoolName=?,province=?,city=?,level=?";
        School school=entry.getValue();
        try(Connect con=new Connect()){
            try(PreparedStatement pstmt=con.getCon().prepareStatement(sql)){
                //insert
                pstmt.setInt(1,school.getSchoolID());
                pstmt.setString(2,school.getSchoolName());
                pstmt.setString(3,school.getProvince());
                pstmt.setString(4,school.getCity());
                pstmt.setString(5,school.getLevel());

                //update
                pstmt.setInt(6,school.getSchoolID());
                pstmt.setString(7,school.getSchoolName());
                pstmt.setString(8,school.getProvince());
                pstmt.setString(9,school.getCity());
                pstmt.setString(10,school.getLevel());

                if(pstmt.executeUpdate()==0)
                    System.out.printf("School not found");
            }catch (SQLException e){
                throw new CacheWriterException("Fail to update/insert school with schoolID:   "+entry.getKey());
            }
        }
    }

    @Override
    public void writeAll(Collection<Cache.Entry<? extends Integer, ? extends School>> entries) {
        //PostgreSQL special statement
        //if school already exists,update its info,else,insert it into database
        String sql="INSERT INTO School (schoolID,schoolName,province,city,level)" +
                "VALUES (?,?,?,?,?) ON CONFLICT(schoolID) " +
                "DO UPDATE SET schoolID=?,schoolName=?,province=?,city=?,level=?";
        try(Connect con=new Connect()){
            try(PreparedStatement pstmt=con.getCon().prepareStatement(sql)){
                for(Cache.Entry<? extends Integer, ? extends School> entry:entries){
                    School school=entry.getValue();

                    //insert
                    pstmt.setInt(1,school.getSchoolID());
                    pstmt.setString(2,school.getSchoolName());
                    pstmt.setString(3,school.getProvince());
                    pstmt.setString(4,school.getCity());
                    pstmt.setString(5,school.getLevel());

                    //update
                    pstmt.setInt(6,school.getSchoolID());
                    pstmt.setString(7,school.getSchoolName());
                    pstmt.setString(8,school.getProvince());
                    pstmt.setString(9,school.getCity());
                    pstmt.setString(10,school.getLevel());

                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }catch (SQLException e){
                throw new CacheWriterException("Fail to writeAll:   "+entries);
            }
        }
    }

    @Override
    public void delete(Object o) throws CacheWriterException {
        String sql="DELETE FROM School WHERE schoolID=?";
        try(Connect con=new Connect()){
            try(PreparedStatement pstmt=con.getCon().prepareStatement(sql)){
                pstmt.setInt(1,(Integer)o);
                if(pstmt.executeUpdate()==0)
                    System.out.printf("School not found");
            }catch (SQLException e){
                throw new CacheWriterException("Fail to delete school:  "+o,e);
            }
        }
    }

    @Override
    public void deleteAll(Collection<?> keys) {
        String sql="DELETE FROM School WHERE schoolID=?";
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
