package node;

import model.School;
import model.Student;
import model.Teacher;
import modelStore.SchoolStore;
import modelStore.StudentStore;
import modelStore.TeacherStore;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;

import javax.cache.configuration.FactoryBuilder;

public class node_liang {
    private static long FLUSH_FREQUENCY=20000;
    private static long THREAD_SLEEP_TIME=10000;

    public static void main(String[] args) {
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
        ipFinder.setMulticastGroup("228.10.10.157");
        spi.setIpFinder(ipFinder);
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setDiscoverySpi(spi);

        Ignite ignite=Ignition.start(cfg);

        //school
        CacheConfiguration<Integer, School> schoolCFG=new CacheConfiguration<>("schoolCache");
        schoolCFG.setCacheStoreFactory(FactoryBuilder.factoryOf(SchoolStore.class));
        schoolCFG.setReadThrough(true);
        schoolCFG.setWriteThrough(true);
        schoolCFG.setWriteBehindEnabled(true);
        schoolCFG.setWriteBehindFlushFrequency(FLUSH_FREQUENCY);
        IgniteCache<Integer,School> schoolCache=ignite.getOrCreateCache(schoolCFG);
        Thread school=new Thread(()->schoolCache.loadCache(null));
        school.start();

        //student
        CacheConfiguration<AffinityKey<Integer>, Student> studentCFG=new CacheConfiguration<>("studentCache");
        studentCFG.setCacheStoreFactory(FactoryBuilder.factoryOf(StudentStore.class));
        studentCFG.setReadThrough(true);
        studentCFG.setWriteThrough(true);
        studentCFG.setWriteBehindEnabled(true);
        studentCFG.setWriteBehindFlushFrequency(FLUSH_FREQUENCY);
        IgniteCache<AffinityKey<Integer>,Student> studentCache=ignite.getOrCreateCache(studentCFG);
        Thread student=new Thread(()->studentCache.loadCache(null));
        student.start();

        //teacher
        CacheConfiguration<AffinityKey<Integer>, Teacher> teacherCFG=new CacheConfiguration<>("teacherCache");
        teacherCFG.setCacheStoreFactory(FactoryBuilder.factoryOf(TeacherStore.class));
        teacherCFG.setReadThrough(true);
        teacherCFG.setWriteThrough(true);
        teacherCFG.setWriteBehindEnabled(true);
        teacherCFG.setWriteBehindFlushFrequency(FLUSH_FREQUENCY);
        IgniteCache<AffinityKey<Integer>,Teacher> teacherCache=ignite.getOrCreateCache(teacherCFG);
        Thread teacher=new Thread(()->teacherCache.loadCache(null));
        teacher.start();

        Thread count=new Thread(
                ()->{
                    while(true){
                        try{
                            Thread.sleep(THREAD_SLEEP_TIME);
                            System.out.println("SCHOOL:  "+schoolCache.localSize());
                            System.out.println("STUDENT:  "+studentCache.localSize());
                            System.out.println("TEACHER:  "+teacherCache.localSize());
                        }catch (InterruptedException e){
                            e.printStackTrace();
                        }
                    }
                }
        );
        count.start();
    }
}

