package Query;

import model.Student;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import java.util.List;

public class SqlQueryTest {
    public static void main(String[] args) {
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
        ipFinder.setMulticastGroup("228.10.10.157");
        spi.setIpFinder(ipFinder);
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setDiscoverySpi(spi);
        cfg.setClientMode(true);

        try(Ignite ignite= Ignition.start(cfg)){
            IgniteCache<AffinityKey<Integer>, Student> studentCache=ignite.cache("studentCache");
            SqlFieldsQuery sql=new SqlFieldsQuery(
                    "SELECT studentName,schoolName " +
                            "FROM Student AS stu,\"schoolCache\".School AS sch " +
                            "WHERE stu.schoolID=sch.schoolID " +
                            "AND stu.schoolID=?"
            ,true);
            long start=System.currentTimeMillis(),finish=0;
            try(QueryCursor<List<?>> cursor=studentCache.query(sql.setArgs(2700))){
                int i=1;
                for(List<?> row:cursor){
                    if(i==1){
                        finish=System.currentTimeMillis();
                        i=0;
                    }
                    System.out.println("studentName:"+row.get(0)+",schoolName:"+row.get(1));
                }
            }
            System.out.println("TOTAL TIME: "+(finish-start));
        }
    }
}
