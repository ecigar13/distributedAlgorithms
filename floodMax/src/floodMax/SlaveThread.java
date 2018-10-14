package floodMax;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import message.Message;

public class SlaveThread implements Runnable {
  protected String name;

  protected static boolean terminated;
  protected static ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> globalIdAndMsgQueueMap;

  protected int myId;
  protected int myMaxUid;
  protected int round;
  private MasterThread masterNode;

  private int myParent = -1;
  protected HashSet<Integer> children = new HashSet<Integer>();
  private HashSet<Integer> neighborSet = new HashSet<Integer>();

  protected ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> localMessagesToSend = new ConcurrentHashMap<>();
  protected LinkedBlockingQueue<Message> localMessageQueue = new LinkedBlockingQueue<>();
  protected boolean newInfo;

  private int nackCount;
  private int ackCount;

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
    this.newInfo = true;
    this.round = 0;
    this.nackCount = 0;
    this.ackCount = 0;

    name = "Thread_" + id;

    SlaveThread.globalIdAndMsgQueueMap = globalIdAndMsgQueueMap;
    SlaveThread.terminated = false;

  }

  public void fillLocalMessagesToSend() {
    System.err.println("Filling localMessagesToSend.");
    localMessagesToSend.put(0, new LinkedBlockingQueue<Message>());
    for (int i : neighborSet) {
      localMessagesToSend.put(i, new LinkedBlockingQueue<Message>());
    }
  }

  /**
   * Put round done message to local queue.
   */
  public void sendRoundDoneToMaster() {
    try {
      localMessagesToSend.get(masterNode.getId()).put(new Message(myId, this.round + 1, this.myMaxUid, "Done"));

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Process message types: Terminate, Round_Number, Explore, N_ACK, ACK
   * 
   * @throws InterruptedException
   */
  public synchronized void processMessageTypes() throws InterruptedException {

    while (!localMessageQueue.isEmpty()) {
      Message msg = localMessageQueue.take();
      // System.err.println(name + " " + msg);
      round = msg.getRound();

      // first round means there's no message to process (except Round_Number, which
      // only updates rounds)
      if (msg.getmType().equals("Terminate")) {
        processTerminateMessage();
        break;

      } else if (msg.getmType().equals("Round_Number")) {
        round = msg.getRound();
      } else if (msg.getmType().equals("Explore")) {

        // process Explore msg (increment round number inside)
        try {
          processExploreMsg(msg);
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else if (msg.getmType().equals("N_ACK")) {
        nackCount++;
        myMaxUid = msg.getMaxUid();
        newInfo = true;
      } else if (msg.getmType().equals("ACK")) {
        ackCount++;
      }
    }
  }

  public void run() {
    System.out.println("Thread start: " + name + " round " + round + " leader " + myMaxUid + " parent " + myParent);

    // base case, check Nack and Ack count first.
    Message temp = countNackAck();

    try {
      if (temp == null) {
      } else if (temp.getmType() == "Leader") {
        // if the message is "Leader", send Leader message to masterNode
        localMessagesToSend.get(masterNode.getId()).put(temp);
        drainToGlobalQueue();
        return;
      } else {
        // if message is anything but leader, send ACK message to parent queue
        localMessagesToSend.get(myParent).put(temp);
      }
      // if Nack, Ack is not full yet, return null. Therefore, do nothing.

    } catch (Exception e) {
      System.err.println("Parent doesn't exist. Suppressing message.");
    }

    fetchFromGlobalQueue(localMessageQueue);

    // process messages
    try {
      processMessageTypes();
    } catch (Exception e) {
      e.printStackTrace();
    }

    // after done processing incoming messages, send msg for next round
    // send explore messages to all neighbors except parent
    if (newInfo) {
      sendExploreMsgToAllNeighbors();
      newInfo = false;
    }

    // Message to master about Round Completion
    sendRoundDoneToMaster();
    // printMessagesToSendMap(localMessagesToSend);
    // set the queue to global queue

    drainToGlobalQueue();
    localMessageQueue.clear();
    for (Entry<Integer, LinkedBlockingQueue<Message>> e : localMessagesToSend.entrySet()) {
      e.getValue().clear();
    }
    System.out.println("Thread stop: " + name + " round " + round + " leader " + myMaxUid + " parent " + myParent);
    System.out.println();
  }

  /**
   * Problem: why are there so many things in neighborSet?
   */
  public void sendExploreMsgToAllNeighbors() {
    for (int targetNodeId : neighborSet) {
      if (targetNodeId == myParent || targetNodeId == myId) {
        continue;
      }

      try {
        System.out.println("Send explore to " + targetNodeId);
        localMessagesToSend.get(targetNodeId).put(new Message(myId, round, myMaxUid, "Explore"));
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * If myMaxUid is bigger than explore msg uid, then don't do anything. If
   * myMaxUid is smaller, then set parent as the sender and set myMaxUid as the
   * msg uid. If myMaxUid and msgUid is the same, it means I have to pick a parent
   * among senders.
   * 
   * @param msg
   * @throws InterruptedException
   */
  public void processExploreMsg(Message msg) throws InterruptedException {

    if (myMaxUid < msg.getMaxUid()) {
      myMaxUid = msg.getMaxUid();
      myParent = msg.getSenderId();
      newInfo = true;
    } else if (myMaxUid == msg.getMaxUid()) {

      // check which parent node has bigger id and choose a parent
      if (myParent > msg.getSenderId()) {
        // send nack to sender.
        System.out.println("Send N_ACK to " + msg.getSenderId());
        localMessagesToSend.get(msg.getSenderId()).put(new Message(myId, round, myMaxUid, "N_ACK"));

      } else if (myParent < msg.getSenderId() && myParent > 0) {
        // pick a new parent: send nack to parent, set the msg sender as parent.
        System.out.println("Send N_ACK to " + myParent);
        localMessagesToSend.get(myParent).put(new Message(myId, round, myMaxUid, "N_ACK"));
        myParent = msg.getSenderId();
      }

      // else it's the same parent. Do nothing.
    } else {
      System.out.println("Send N_ACK to " + msg.getSenderId());
      localMessagesToSend.get(msg.getSenderId()).put(new Message(myId, round, myMaxUid, "N_ACK"));
      // if my maxUid is
      // bigger, send N_NACK
    }
  }

  /**
   * Check nackCount and ackCount and come up with a message.
   * 
   * @return a message based on the count
   */
  public Message countNackAck() {
    if (nackCount == neighborSet.size() - 1 || nackCount + ackCount == neighborSet.size() - 1) {
      // leaf node, send to parent.
      // internal node, send to parent.

      return new Message(myId, round, myMaxUid, "ACK");
    } else if (ackCount == neighborSet.size()) {
      // leader node. Send leader message to master.
      return new Message(myId, round, myMaxUid, "Leader");
    } else
      return null;
  }

  /**
   * Set status of terminated to stop all threads. Need to implement "print out
   * the tree"
   */
  public void processTerminateMessage() {
    System.out.println("Leader id: " + myMaxUid);

    // Need to implement:
    // Obtaining an iterator for the entry set
    // output the graph and stop execution

    SlaveThread.terminated = true;

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

    // System.err.println("Fetching from global queue. " +
    // globalIdAndMsgQueueMap.get(id).size() + " " + localQ.size());
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
      for (Message m : e.getValue()) {
        System.err.println(e.getValue().size() + " " + e.getKey() + "   " + m);
      }
    }
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

      // can't send message to itself
      if (e.getKey() == myId) {
        continue;
      }

      e.getValue().drainTo(globalIdAndMsgQueueMap.get(e.getKey()));
      // System.err.println(
      // name + " After draining to global queue: " + e.getKey() + " " +
      // localMessagesToSend.get(e.getKey()).size());
      if (!e.getValue().isEmpty()) {
        System.err.println("Queue is not empty at end of round");
      }

      // System.err.println(name + " After draining to global queue: " +
      // localMessagesToSend.get(e.getKey()).size());
    }
  }

  public void setNeighbours(HashSet<Integer> neighbours) {
    this.neighborSet = neighbours;
  }

  public void insertNeighbour(int neighborId) {
    this.neighborSet.add(neighborId);
  }

  public int getId() {
    return myId;
  }

  public void sleep() {
    try {
      Thread.sleep(1500);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public void setName(String name) {
    this.name = name;
  }
}