package com.github.xgp.cluster.jgroups;

import static com.github.xgp.cluster.jgroups.TestSupport.await;
import static com.github.xgp.cluster.jgroups.TestSupport.quietPeriod;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.github.xgp.cluster.examples.JGroupsClusterExample;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jgroups.JChannel;
import org.junit.After;
import org.junit.Test;

/**
 * Runs the example app in separate JVMs over TCP and drives it through stdin, asserting on its own
 * console output. This is the only tier that crosses JVM boundaries, where JGroups logical names
 * are not shared through the static NameCache. Run with {@code mvn verify -Pmultiprocess}.
 */
public class JGroupsMultiProcessIT {

  private static final Pattern NODE_LINE = Pattern.compile("^(?:> )*\\s+- (\\S+)( \\(coordinator\\))?$");

  /** One example-app process with its stdout captured line by line. */
  static class Peer {
    final String name;
    final Process process;
    final PrintWriter stdin;
    final List<String> out = new CopyOnWriteArrayList<>();
    final List<String> err = new CopyOnWriteArrayList<>();

    Peer(String name, Process process) {
      this.name = name;
      this.process = process;
      this.stdin =
          new PrintWriter(
              new java.io.OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8),
              true);
      drain(process.getInputStream(), out, name + "-out");
      drain(process.getErrorStream(), err, name + "-err");
    }

    private static void drain(InputStream in, List<String> sink, String threadName) {
      Thread t =
          new Thread(
              () -> {
                try (BufferedReader r =
                    new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                  String line;
                  while ((line = r.readLine()) != null) {
                    sink.add(line);
                  }
                } catch (Exception e) {
                  // process ended
                }
              },
              threadName);
      t.setDaemon(true);
      t.start();
    }

    void command(String line) {
      stdin.println(line);
    }

    int count(String fragment) {
      int n = 0;
      for (String l : out) {
        if (l.contains(fragment)) {
          n++;
        }
      }
      return n;
    }

    boolean saw(String fragment) {
      return count(fragment) > 0;
    }

    /** Issues {@code nodes} and returns the listed names, marking the coordinator with '*'. */
    List<String> listNodes(int expectedCount) {
      int start = out.size();
      command("nodes");
      List<String> result = new ArrayList<>();
      await(
          name + " lists " + expectedCount + " nodes",
          5000,
          () -> {
            result.clear();
            boolean inListing = false;
            for (int i = start; i < out.size(); i++) {
              String l = out.get(i);
              if (l.contains("Current cluster nodes:")) {
                inListing = true;
                continue;
              }
              Matcher m = NODE_LINE.matcher(l);
              if (inListing && m.matches()) {
                result.add(m.group(1) + (m.group(2) != null ? "*" : ""));
              }
            }
            return result.size() >= expectedCount;
          });
      return new ArrayList<>(result);
    }

    @Override
    public String toString() {
      return name + " stdout=" + out + "\nstderr(tail)=" + tail(err, 40);
    }

    private static List<String> tail(List<String> l, int n) {
      return l.subList(Math.max(0, l.size() - n), l.size());
    }
  }

  private final List<Peer> peers = new ArrayList<>();

  private static String codeSource(Class<?> c) throws Exception {
    return new File(c.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
  }

  private Peer launch(String name) throws Exception {
    Set<String> cp = new LinkedHashSet<>();
    cp.add(codeSource(JGroupsClusterExample.class)); // target/classes
    cp.add(codeSource(JGroupsMultiProcessIT.class)); // target/test-classes (test-tcp.xml)
    cp.add(codeSource(JChannel.class)); // the jgroups jar
    String java = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
    ProcessBuilder pb =
        new ProcessBuilder(
            java,
            "-Djgroups.bind_addr=127.0.0.1",
            "-Djava.net.preferIPv4Stack=true",
            "-cp",
            String.join(File.pathSeparator, cp),
            JGroupsClusterExample.class.getName(),
            name,
            TestSupport.TCP_CONFIG);
    Peer p = new Peer(name, pb.start());
    peers.add(p);
    await(name + " started", 30000, () -> p.saw("Cluster node started"));
    return p;
  }

  @After
  public void killAll() throws Exception {
    for (Peer p : peers) {
      p.process.destroy();
    }
    for (Peer p : peers) {
      if (!p.process.waitFor(5, TimeUnit.SECONDS)) {
        p.process.destroyForcibly();
      }
    }
  }

  private static void awaitAll(String what, List<Peer> ps, java.util.function.Predicate<Peer> c) {
    for (Peer p : ps) {
      await(what + " on " + p.name + "\n" + p, 20000, () -> c.test(p));
    }
  }

  @Test
  public void exampleAppFormsAClusterAcrossJvms() throws Exception {
    Peer p1 = launch("node1");
    Peer p2 = launch("node2");
    Peer p3 = launch("node3");
    List<Peer> all = Arrays.asList(p1, p2, p3);

    // membership, as seen by members that were already running
    await("node1 sees node2 join\n" + p1, 20000, () -> p1.saw("[CLUSTER] Node added: node2"));
    await("node1 sees node3 join\n" + p1, 20000, () -> p1.saw("[CLUSTER] Node added: node3"));
    await("node2 sees node3 join\n" + p2, 20000, () -> p2.saw("[CLUSTER] Node added: node3"));

    // every process lists all three, with the same single coordinator
    Set<String> coordinators = new TreeSet<>();
    for (Peer p : all) {
      List<String> listing = p.listNodes(3);
      Set<String> names = new TreeSet<>();
      int flagged = 0;
      for (String n : listing) {
        if (n.endsWith("*")) {
          flagged++;
          coordinators.add(n.substring(0, n.length() - 1));
        }
        names.add(n.replace("*", ""));
      }
      assertEquals(p.name + " listing " + listing, new TreeSet<>(Arrays.asList("node1", "node2", "node3")), names);
      assertEquals(p.name + " flags exactly one coordinator: " + listing, 1, flagged);
    }
    assertEquals("all processes agree on the coordinator", 1, coordinators.size());

    // broadcast: exactly once in every process, including the sender
    p1.command("send hi");
    awaitAll("broadcast", all, p -> p.saw("[MESSAGE from node1] hi"));

    // direct: only on the target
    p1.command("sendto node2 psst");
    await("direct on node2\n" + p2, 20000, () -> p2.saw("[DIRECT from node1] psst"));

    quietPeriod(1000);
    for (Peer p : all) {
      assertEquals(p.toString(), 1, p.count("[MESSAGE from node1] hi"));
    }
    assertEquals(1, p2.count("[DIRECT from node1] psst"));
    assertEquals(0, p1.count("[DIRECT from node1]"));
    assertEquals(0, p3.count("[DIRECT from node1]"));

    // clean leave
    p2.command("quit");
    assertTrue("node2 exits after quit", p2.process.waitFor(20, TimeUnit.SECONDS));
    List<Peer> survivors = Arrays.asList(p1, p3);
    awaitAll("node2 removal", survivors, p -> p.saw("[CLUSTER] Node removed: node2"));
    for (Peer p : survivors) {
      assertEquals(p.toString(), 0, p.count("[CLUSTER] Node failed: node2"));
      List<String> listing = p.listNodes(2);
      assertEquals(p.name + " listing " + listing, 2, listing.size());
    }

    p1.command("quit");
    p3.command("quit");
    assertTrue(p1.process.waitFor(20, TimeUnit.SECONDS));
    assertTrue(p3.process.waitFor(20, TimeUnit.SECONDS));
  }
}
