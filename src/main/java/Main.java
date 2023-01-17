import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.InFlightRequestPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Main {
    private final static String keyspaceName = "ks";
    private final static String tableName = "t";

    private final static String createKeyspace = String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}", keyspaceName);
    private final static String createTable = String.format("CREATE TABLE %s.%s (pk int, v int, primary key (pk, v))", keyspaceName, tableName);
    private final static String useKs = String.format("USE %s", keyspaceName);
    private final static String insertInto = String.format("INSERT INTO %s (pk, v) VALUES (?, ?)", tableName);

    private static volatile boolean shutdown = false;

    private enum Workload {
        INSERT, TOKEN, INFLIGHT, ROUND
    }

    public static void main(String[] args) throws InterruptedException {
        Namespace parsedArgs = parseArguments(args);
        final List<Integer> pks = parsedArgs.getList("pks").stream().map((s) -> Integer.parseInt((String) s)).collect(Collectors.toList()); // default 0 1 2
        final Workload workload = Workload.valueOf(parsedArgs.getString("workload").toUpperCase());
        final int concurrency = Integer.parseInt(parsedArgs.getString("concurrency"));
        final List<String> contactPoints = parsedArgs.getList("ip");

        Cluster.Builder clusterBuilder = Cluster.builder().addContactPoints(contactPoints.toArray(new String[0]))
                .withoutAdvancedShardAwareness();

        List<Thread> threads = new ArrayList<>();

        switch (workload) {
            case INSERT:
                // Create keyspace, table and populate the db
                createDb(clusterBuilder);
                break;
            case TOKEN:
                final Session tokenAwareSession = clusterBuilder
                        .withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy())).build().connect();
                threads.addAll(benchmark(tokenAwareSession, concurrency, pks));
                break;
            case INFLIGHT:
                final Session inflightRequestSession = clusterBuilder
                        .withLoadBalancingPolicy(new InFlightRequestPolicy(new RoundRobinPolicy())).build().connect();
                threads.addAll(benchmark(inflightRequestSession, concurrency, pks));
                break;
            case ROUND:
                final Session roundSession = clusterBuilder.withLoadBalancingPolicy(new RoundRobinPolicy()).build().connect();
                threads.addAll(benchmark(roundSession, concurrency, pks));
                break;
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Received termination signal, shutting down!");
            shutdown = true;
        }));

        for (Thread thread : threads) {
            thread.join();
        }
    }

    private static List<Thread> benchmark(Session session, int concurrency, List<Integer> pks) throws InterruptedException {
        final String select = String.format("SELECT * FROM %s.%s WHERE pk = :pk and v > :v", keyspaceName, tableName);
        final PreparedStatement preparedSelect = session.prepare(select);
        final List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < concurrency; i++) {
            Thread t = new Thread(() -> {
                while (!shutdown) {
                    List<ResultSetFuture> results = new ArrayList<>();
                    for (int pk: pks) {
                        ResultSetFuture resultSetFuture = session.executeAsync(preparedSelect.bind(pk, 995));
                        results.add(resultSetFuture);
                    }

                    try {
                        for (ResultSetFuture result: results) {
                            result.get();
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        System.out.println(Thread.currentThread().getName() + ": Getting result future failed! - " + e.getMessage());
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                }
            });

            threads.add(t);
        }

        for (Thread t : threads) {
            t.start();
            Thread.sleep(100);
        }

        return threads;
    }

    private static void createDb(Cluster.Builder clusterBuilder) {
        final Session session = clusterBuilder.build().connect();
        session.execute(createKeyspace);
        session.execute(useKs);
        session.execute(createTable);
        final PreparedStatement preparedInsert = session.prepare(insertInto);

        // Insert some values with pk range from 0 to 9
        for (int pk = 0; pk < 10; pk++) {
            for (int ck = 0; ck < 1000; ck++) {
                session.execute(preparedInsert.bind(pk, ck + 1));
            }
        }

        System.out.println("Writing to database is finished, shutdown!");
    }

    private static Namespace parseArguments(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("./bench").build().defaultHelp(true);
        parser.addArgument("-w", "--workload").required(true).help("Workload: insert, token, inflight");
        parser.addArgument("-c", "--concurrency").required(false).setDefault(1024).help("Concurrency for token and inflight workloads. Default is 1024.");
        parser.addArgument("-p", "--pks").required(false).nargs(3).setDefault(0, 1, 2).help("Partition key");
        parser.addArgument("-i", "--ip").nargs("*").required(true).help("Provide contact points of the cluster");

        try {
            return parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(-1);
            return null;
        }
    }
}
