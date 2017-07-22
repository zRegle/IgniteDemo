package node;

import model.School;
import modelStore.SchoolStore;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;

import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import java.util.List;

public class node_liang {
    private static long FLUSH_FREQUENCY=20000;

    public static void main(String[] args) {
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
        ipFinder.setMulticastGroup("228.10.10.157");
        spi.setIpFinder(ipFinder);
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setDiscoverySpi(spi);

        Ignite ignite=Ignition.start(cfg);

        CacheConfiguration<Integer, School> schoolCFG=new CacheConfiguration<>("schoolCache");
        schoolCFG.setCacheLoaderFactory(FactoryBuilder.factoryOf(SchoolStore.class));
        schoolCFG.setReadThrough(true);
        schoolCFG.setWriteThrough(true);
        schoolCFG.setWriteBehindEnabled(true);
        schoolCFG.setWriteBehindFlushFrequency(FLUSH_FREQUENCY);

        IgniteCache<Integer,School> schoolCache=ignite.getOrCreateCache(schoolCFG);
        schoolCache.loadCache(null);

        List<School> schoolList=schoolCache.query(new ScanQuery<Integer,School>(
                (schoolID,school)->schoolID>99900),
                Cache.Entry::getValue
        ).getAll();

        for(School school:schoolList)
            System.out.println(school.toString());

        System.out.println(schoolCache.size());
    }
}
