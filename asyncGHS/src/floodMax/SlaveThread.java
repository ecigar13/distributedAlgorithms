package floodMax;

import java.util.HashSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.sun.jmx.snmp.Timestamp;

import java.util.Collections;
import java.util.Comparator;

import message.Message;
import sun.reflect.generics.tree.Tree;

public class SlaveThread implements Runnable {
  protected String name;
  protected boolean isLeader = false;
  protected boolean terminated;

  protected int id;
  protected int maxUid;
  protected int round;
  private int myParent;
  protected double mwoe;
  protected Message currentReportMessage;
  protected MasterThread masterNode;

  protected ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> globalIdAndMsgQueueMap;
  protected ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> localMessagesToSend;
  protected LinkedBlockingQueue<Message> localMessageQueue = new LinkedBlockingQueue<>();

  protected HashSet<Integer> receivedConnect = new HashSet<>();
  protected HashSet<Integer> sentConnect = new HashSet<>();

  protected TreeSet<Integer> reportReceived = new TreeSet<>();

  protected TreeMap<Integer, Double> neighborMap = new TreeMap<>();
  protected TreeSet<Integer> branch = new TreeSet<>();
  protected TreeSet<Integer> rejected = new TreeSet<>();
  protected TreeSet<Integer> basic = new TreeSet<>();

  public SlaveThread() {
  }

  /**
   * Constructor.
   * 
   * @param id
   * @param masterNode
   */

  public SlaveThread(int id, MasterThread masterNode,
      ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> globalIdAndMsgQueueMap) {
    this.id = id;
    this.maxUid = id;
    this.myParent = -1;
    this.mwoe = Double.MAX_VALUE;
    this.masterNode = masterNode;
    this.round = 0;
    this.terminated = false;

    name = "Thread_" + id;

    this.globalIdAndMsgQueueMap = globalIdAndMsgQueueMap;
    this.localMessagesToSend = new ConcurrentHashMap<>();
    // init local messages to send will be done after construction. It's messy for
    // now.
  }

  /**
   * Process messages int the queue.
   */
  public void processMessageTypes() throws InterruptedException {
    Message m;
    while (localMessageQueue.size() != 0) {
      m = localMessageQueue.poll();

      if (m.getmType().equals("initiate")) {
        processInitMessage(m);
      } else if (m.getmType().equals("report")) {
        processReportMessage(m);
      } else if (m.getmType().equals("test")) {
        processTest(m);
      } else if (m.getmType().equals("accept")) {
        basic.remove(m.getSenderId());
        // what to do here?
      } else if (m.getmType().equals("reject")) {
        rejected.add(m.getSenderId());
      } else if (m.getmType().equals("changeRoot")) {
        // what to do here?
      } else if (m.getmType().equals("connect")) {
        processConnectMessage(m);
      }
    }
  }

  /**
   * If I have only 1 branch and no basic. Then I'm the leaf. Send report back to
   * parent. If I have more than 1 branch, I'm an internal node. Send initiate to
   * all branches (except parent) Ignore reject. Wait for report from children and
   * use basic edge to find mwoe.
   * 
   * @param m
   */
  public void processInitMessage(Message m) throws InterruptedException {
    if (branch.size() > 1 && basic.size() == 0) {
      for (int i : basic) {
        mwoe = mwoe < neighborMap.get(i) ? mwoe : neighborMap.get(i);
      }

      Message temp = new Message(id, mwoe, round, maxUid, "report");
      temp.getPath().add(id);
      localMessagesToSend.get(myParent).put(temp);
    } else {
      sendInitiateToBranch();
    }
  }

  /**
   * If I got all reports from all my children, it'll take minimum of
   * Union(children's report, basic branches) and send it to my parent. Ignore
   * reject branches. Set mwoe to infinite once done.
   * 
   * Leader has the ability to broadcast connect.
   * 
   * @param m
   */
  public void processReportMessage(Message m) throws InterruptedException {
    if (mwoe > m.getMwoe()) {
      mwoe = m.getMwoe();
      reportReceived.add(m.getSenderId());
      currentReportMessage = m;
    }

    // go through basic edges and find the mwoe. Compare it with what I get from
    // reports.
    if (reportReceived.size() == branch.size()) {
      int tempNeighbor = id;
      for (int i : basic) {
        if (mwoe > neighborMap.get(i)) {
          tempNeighbor = i;
          mwoe = neighborMap.get(i);
        }
      }

      if (isLeader) {
        broadcastConnect(m, tempNeighbor);
      } else if (tempNeighbor != id) {
        // if basic branches have less weight than mwoe
        Message temp = new Message(id, mwoe, round, maxUid, "report");
        temp.getPath().add(tempNeighbor);
        temp.getPath().add(id);
        localMessagesToSend.get(myParent).put(temp);
      } else {
        m.setSenderId(id);
        m.getPath().add(id);
        localMessagesToSend.get(myParent).put(m);
      }
      reportReceived.clear();
      mwoe = Double.MAX_VALUE;
    }

  }

  /**
   * Implement: If I'm in another component and I get connect, decide if I want to
   * connect.
   * 
   * If I receive a connect message from my component, then I'll relay it to the
   * next children on the path.
   * 
   */
  public void processConnectMessage(Message m) throws InterruptedException, NullPointerException {
    if (maxUid != m.getMaxUid()) {
      // I'm in another component. What do I do?
    } else {
      // I'm in the same component, keep sending it.
      int temp = m.getPath().removeLast();
      m.setSenderId(id);
      localMessagesToSend.get(temp).put(m);
    }

  }

  /**
   * Send the message back down the path.
   * 
   * @param m
   * @param id
   * @throws InterruptedException
   */
  public void broadcastConnect(Message m, int id) throws InterruptedException {
    Message temp = new Message(id, mwoe, round, maxUid, "connect");
    if (basic.contains(id)) {
      localMessagesToSend.get(id).put(temp);
    }
    m.setMwoe(mwoe);
    m.setmType("connect");
    m.setSenderId(id);

    localMessagesToSend.get(m.getPath().removeLast()).put(m);

  }

  /**
   * Process Test messages.
   * 
   * @param m
   */
  public void processTest(Message m) throws InterruptedException {
    if (maxUid == m.getMaxUid()) {
      localMessagesToSend.get(m.getSenderId()).put(new Message(id, m.getMwoe(), m.getRound(), maxUid, "reject"));

    } else if (maxUid != m.getMaxUid() && round <= m.getRound()) {
      maxUid = m.getMaxUid();
      localMessagesToSend.get(m.getSenderId()).put(new Message(id, m.getMwoe(), m.getRound(), maxUid, "accept"));
    } else if (maxUid != m.getMaxUid() && round > m.getRound()) {
      // wait until my round == level of the other node then send reject/accept.
      // This means do nothing.
    }
  }

  /**
   * Implement
   * 
   * @param m
   */
  public void childAbsorb(Message m) {
  }

  /**
   * Implement
   * 
   * @param m
   */
  public void parentAbsorb(Message m) {

  }

  /**
   * Implement. Merge is done by both child and parent.
   */
  public void merge(Message m) {
    if (sentConnect.contains(m.getSenderId()) && receivedConnect.contains(m.getSenderId())) {
      if (maxUid < m.getMaxUid()) {
        maxUid = m.getMaxUid();
        myParent = m.getSenderId();
      }
      branch.add(m.getSenderId());
      basic.remove(m.getSenderId());
      // broadcast change root to the rest of the tree, change their maxUid to this
      // maxUid.
    }
  }

  /**
   * Send initiate to all branches except parent.
   */
  public void sendInitiateToBranch() throws InterruptedException {
    for (Entry<Integer, Double> e : neighborMap.entrySet()) {
      if (e.getKey() == myParent) {
        continue;
      }

      System.out.println("Send initiate to " + e.getKey());
      localMessagesToSend.get(e.getKey()).put(new Message(id, 0, round, maxUid, "initiate"));
    }
  }

  /**
   * Send test messages to all neighbors except parent.
   * 
   * @throws InterruptedException
   */
  public void sendTestToNeighbors() throws InterruptedException {
    for (Entry<Integer, Double> e : neighborMap.entrySet()) {
      if (e.getKey() == myParent) {
        continue;
      }

      System.out.println("Send test to " + e.getKey());
      localMessagesToSend.get(e.getKey()).put(new Message(id, mwoe, round, maxUid, "test"));
    }
  }

  /**
   * Need to implement this. Don't use while loop.
   */
  public void run() {

    System.out.println(name + " start. round " + round + " leader " + maxUid + " parent " + myParent + " "
        + branch.size() + " " + rejected.size() + " " + basic.size());
    try {
      if (!terminated) {
        fetchFromGlobalQueue(localMessageQueue);
        processMessageTypes();

        // after done processing incoming messages, send msg for next round
        sendInitiateToBranch();
        if (round != 0)
          sendRoundDoneToMaster();
        // then master will drain to its own queue.

      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println(name + " stop. round " + round + " leader " + maxUid + " parent " + myParent + " "
        + branch.size() + " " + rejected.size() + " " + basic.size());
    System.out.println(branch.toString());
    System.out.println(rejected.toString());
    System.out.println(basic.toString());
    System.out.println();

  }

  /**
   * Drain the local msg queue and put all in the global queue. The local queue
   * should be empty after this. This should be done at the end of each round.
   * e.g. each thread has a ConcurrentHashMap similar to the global one, but
   * should be empty. At end of each round, it unloads the queues to the global
   * ConcurrentHashMap.
   */
  public synchronized void drainToGlobalQueue() {

    for (Entry<Integer, LinkedBlockingQueue<Message>> e : localMessagesToSend.entrySet()) {
      e.getValue().drainTo(globalIdAndMsgQueueMap.get(e.getKey()));

      if (!e.getValue().isEmpty()) {
        System.err.println("Queue is not empty at end of round");
      }

    }
  }

  /**
   * Set status of terminated to stop all threads. Need to implement "print out
   * the tree"
   */
  public void processTerminateMessage() {
    System.out.println("Thread terminated. Leader id: " + maxUid);
    this.terminated = true;

  }

  /**
   * Drain the global queue to this local queue. The Global queue will have zero
   * element.
   * 
   * @return
   */
  public void fetchFromGlobalQueue(LinkedBlockingQueue<Message> localQ) {
    if (!globalIdAndMsgQueueMap.get(id).isEmpty()) {
      globalIdAndMsgQueueMap.get(id).drainTo(localQ);
    }

    if (!globalIdAndMsgQueueMap.get(id).isEmpty()) {
      System.err.println("Global queue is not empty. We have a prolem.");
    }
  }

  /**
   * Print out the hashmap/queue to debug.
   * 
   * @param hm
   */
  public synchronized void printMessagesToSendMap(ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> hm) {
    for (Entry<Integer, LinkedBlockingQueue<Message>> e : hm.entrySet()) {
      System.err.println(e.getValue().size() + " " + e.getKey() + " " + e.getValue());
    }
    sleep();
  }

  public void setNeighbours(TreeMap<Integer, Double> neighborMap) {
    this.neighborMap = neighborMap;
  }

  public void insertNeighbour(int neighborId, double distance) {
    this.neighborMap.put(neighborId, distance);
  }

  public int getId() {
    return id;
  }

  public void sleep() {
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  /**
   * Initiate the local queue. Otherwise it will throw error.
   */
  public void initLocalMessagesToSend() {
    localMessagesToSend.put(masterNode.getId(), new LinkedBlockingQueue<Message>());
    for (Entry<Integer, Double> e : neighborMap.entrySet()) {
      localMessagesToSend.put(e.getKey(), new LinkedBlockingQueue<Message>());
    }
  }

  /**
   * Put round done message to local queue.
   */
  public void sendRoundDoneToMaster() {
    try {
      localMessagesToSend.get(masterNode.getId()).put(new Message(id, mwoe, round, maxUid, "Done"));

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getMyParent() {
    return myParent;
  }

  public TreeMap<Integer, Double> getNeighborMap() {
    return neighborMap;
  }

}