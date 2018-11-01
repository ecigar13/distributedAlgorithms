package floodMax;

import java.util.HashSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Collections;

import message.Message;
import sun.reflect.generics.tree.Tree;

public class SlaveThread implements Runnable {
  protected String name;

  protected boolean terminated;

  protected int myId;
  protected int myMaxUid;
  protected int round;
  private int myParent = -1;
  protected MasterThread masterNode;

  protected ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> globalIdAndMsgQueueMap;
  protected ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> localMessagesToSend;
  protected LinkedBlockingQueue<Message> localMessageQueue = new LinkedBlockingQueue<>();

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
    this.myId = id;
    this.myMaxUid = id;
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
  public void processMessageTypes(Message m) {
    if (m.getmType().equals("initiate")) {

    } else if (m.getmType().equals("report")) {

    } else if (m.getmType().equals("test")) {

    } else if (m.getmType().equals("accept")) {

    } else if (m.getmType().equals("reject")) {

    } else if (m.getmType().equals("changeRoot")) {

    } else if (m.getmType().equals("connect")) {

    }

    // initiate
    // report
    // test
    // accept
    // reject
    // changeroot
    // connect
  }

  /**
   * Implement. Merge is done by both child and parent.
   */
  public void merge(int clusterId, int parentId) {
    // Done by child.
    // implement merge done by parent.
  }

  /**
   * Implement. Absorb is done by both child and parent.
   */
  public void absorb(int clusterId, int parentId) {
  }

  /**
   * Need to implement this. Don't use while loop.
   */
  public void run() {

    System.out.println(name + " start. round " + round + " leader " + myMaxUid + " parent " + myParent + " "
        + branch.size() + " " + rejected.size() + " " + basic.size());
    try {
      if (!terminated) {
        fetchFromGlobalQueue(localMessageQueue);
        processMessageTypes();

        // after done processing incoming messages, send msg for next round
        sendMwoeMsgToAllNeighbors();
        if (round != 0)
          sendRoundDoneToMaster();
        // then master will drain to its own queue.

      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println(name + " stop. round " + round + " leader " + myMaxUid + " parent " + myParent + " "
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
   * Problem: why are there so many things in neighborSet?
   */
  public void sendMwoeMsgToAllNeighbors() {
    for (Entry<Integer, Double> targetNode : neighborMap.entrySet()) {
      if (targetNode.getKey() == myId) {
        continue;
      }

      try {
        System.out.println("Send explore to " + targetNode);
        localMessagesToSend.get(targetNode.getKey()).put(new Message(myId, null, round, myMaxUid, "Explore"));
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Set status of terminated to stop all threads. Need to implement "print out
   * the tree"
   */
  public void processTerminateMessage() {
    System.out.println("Thread terminated. Leader id: " + myMaxUid);
    this.terminated = true;

  }

  /**
   * Drain the global queue to this local queue. The Global queue will have zero
   * element.
   * 
   * @return
   */
  public void fetchFromGlobalQueue(LinkedBlockingQueue<Message> localQ) {
    if (!globalIdAndMsgQueueMap.get(myId).isEmpty()) {
      globalIdAndMsgQueueMap.get(myId).drainTo(localQ);
    }

    if (!globalIdAndMsgQueueMap.get(myId).isEmpty()) {
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
    return myId;
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
    for (int i : neighborSet) {
      localMessagesToSend.put(i, new LinkedBlockingQueue<Message>());
    }
  }

  /**
   * Put round done message to local queue.
   */
  public void sendRoundDoneToMaster() {
    try {
      localMessagesToSend.get(masterNode.getId()).put(new Message(myId, null, round, myMaxUid, "Done"));

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