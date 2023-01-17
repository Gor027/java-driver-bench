import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;
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
        INSERT, SELECT
    }

    public static void main(String[] args) throws InterruptedException {
        Namespace parsedArgs = parseArguments(args);
        final List<Integer> pks = parsedArgs.getList("pks").stream().map((s) -> Integer.parseInt((String) s)).collect(Collectors.toList()); // default 0 1 2
        final Workload workload = Workload.valueOf(parsedArgs.getString("workload").toUpperCase());
        final int concurrency = Integer.parseInt(parsedArgs.getString("concurrency"));
        final List<String> contactPoints = parsedArgs.getList("ip");
        final Collection<InetSocketAddress> socketAddresses = new ArrayList<>();

        for (String address: contactPoints) {
            socketAddresses.add(new InetSocketAddress(address, 9042));
        }

        final CqlSession session = new CqlSessionBuilder().withLocalDatacenter("datacenter1").addContactPoints(socketAddresses).build();

        List<Thread> threads = new ArrayList<>();

        switch (workload) {
            case INSERT:
                // Create keyspace, table and populate the db
                createDb(session);
                break;
            case SELECT:
                threads.addAll(benchmark(session, concurrency, pks));
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

    private static List<Thread> benchmark(CqlSession session, int concurrency, List<Integer> pks) {
        final String select = String.format("SELECT * FROM %s.%s WHERE pk = :pk and v > :v", keyspaceName, tableName);
        final PreparedStatement preparedSelect = session.prepare(select);
        final List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < concurrency; i++) {
            Thread t = new Thread(() -> {
                while (!shutdown) {
                    List<CompletionStage<AsyncResultSet>> results = new ArrayList<>();
                    for (int pk: pks) {
                        // Note: Values MUST be bound each time to avoid sending the same node because of paging request optimization
                        CompletionStage<AsyncResultSet> resultSetFuture = session.executeAsync(preparedSelect.bind(pk, 995));
                        results.add(resultSetFuture);
                    }

                    try {
                        for (CompletionStage<AsyncResultSet> result: results) {
                            result.toCompletableFuture().get();
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
        }

        return threads;
    }

    private static void createDb(CqlSession session) {
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
