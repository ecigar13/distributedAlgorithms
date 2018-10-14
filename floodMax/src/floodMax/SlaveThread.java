package floodMax;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import javax.sound.midi.SysexMessage;

import com.sun.corba.se.spi.servicecontext.SendingContextServiceContext;
import com.sun.org.apache.bcel.internal.generic.IF_ACMPEQ;

import javafx.scene.Parent;
import message.Message;

public class SlaveThread implements Runnable {
  protected String name;

  protected boolean terminated;
  protected static ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> globalIdAndMsgQueueMap;

  protected int myId;
  protected int myMaxUid;
  protected int round;
  private int myParent = -1;
  private MasterThread masterNode;

  protected HashSet<Integer> ackReceived = new HashSet<Integer>();
  protected HashSet<Integer> nackReceived = new HashSet<Integer>();
  private HashSet<Integer> neighborSet = new HashSet<Integer>();
  private boolean sentAck;

  protected ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> localMessagesToSend = new ConcurrentHashMap<>();
  protected LinkedBlockingQueue<Message> localMessageQueue = new LinkedBlockingQueue<>();
  protected boolean newInfo;

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
    this.sentAck = false;

    name = "Thread_" + id;

    SlaveThread.globalIdAndMsgQueueMap = globalIdAndMsgQueueMap;
    this.terminated = false;

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

      } else if (myParent > 0 && myParent < msg.getSenderId()) {
        // pick a new parent: send nack to parent, set the msg sender as parent.
        System.out.println("Send N_ACK to " + myParent);
        localMessagesToSend.get(myParent).put(new Message(myId, round, myMaxUid, "N_ACK"));
        myParent = msg.getSenderId();
      }

      // else it's the same parent. Do nothing.
    } else {
      // My maxUid is bigger. I'll send rejection.
      System.out.println("Send N_ACK to " + msg.getSenderId());
      localMessagesToSend.get(msg.getSenderId()).put(new Message(myId, round, myMaxUid, "N_ACK"));
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
      System.out.println(name + " processing " + msg);
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
        nackReceived.add(msg.getSenderId());
        if (myMaxUid < msg.getMaxUid()) {
          myMaxUid = msg.getMaxUid();
          myParent = msg.getSenderId();
          newInfo = true;
        }
      } else if (msg.getmType().equals("ACK")) {
        if (!ackReceived.contains(msg.getSenderId())) {
          ackReceived.add(msg.getSenderId());
          if (myParent != -1) {
            msg.setSenderId(myId);
            if (myMaxUid > msg.getMaxUid()) {
              msg.setMaxUid(myMaxUid);
            }
            localMessagesToSend.get(myParent).put(msg);
          }
        }

      }

    }
  }

  /**
   * Check nackCount and ackCount and come up with a message.
   * 
   * @return a message based on the count
   * @throws InterruptedException
   */
  public void countNackAck() throws InterruptedException {

    if (myParent != -1 && (nackReceived.size() == neighborSet.size()
        || nackReceived.size() + ackReceived.size() == neighborSet.size() - 1)) {
      // leaf node, send to parent.
      // internal node, send to parent.
      System.out.println("Sending ACK to parent " + myParent);
      localMessagesToSend.get(myParent).put(new Message(myId, round, myMaxUid, "ACK"));

    } else if (ackReceived.size() == neighborSet.size()) {
      // leader node. Send leader message to master.
      System.out.println("Sending Leader to master.");
      localMessagesToSend.get(masterNode.getId()).put(new Message(myId, round, myMaxUid, "Leader"));
    } else
      return;
  }

  public void run() {
    System.out.println(name + " start. round " + round + " leader " + myMaxUid + " parent " + myParent + " "
        + nackReceived.size() + " " + ackReceived.size() + " " + neighborSet.size());

    if (!terminated) {
      try {
        fetchFromGlobalQueue(localMessageQueue);
        // base case, check Nack and Ack count first.
        countNackAck();
        // process messages
        processMessageTypes();
      } catch (Exception e) {
        e.printStackTrace();
      }
      // after done processing incoming messages, send msg for next round
      if (newInfo) {
        sendExploreMsgToAllNeighbors();
        newInfo = false;
      }

      // Message to master about Round Completion
      sendRoundDoneToMaster();
      // set the queue to global queue

      // printMessagesToSendMap(localMessagesToSend);
      drainToGlobalQueue();
      localMessageQueue.clear();
      for (Entry<Integer, LinkedBlockingQueue<Message>> e : localMessagesToSend.entrySet()) {
        e.getValue().clear();
      }

    }
    System.out.println(name + " stop. round " + round + " leader " + myMaxUid + " parent " + myParent);
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
      // System.err.println("Before: ");
      // System.err.println(e.getKey() + " " +
      // globalIdAndMsgQueueMap.get(e.getKey()));
      e.getValue().drainTo(globalIdAndMsgQueueMap.get(e.getKey()));

      // System.err.println("After: ");
      // System.err.println(e.getKey() + " " +
      // globalIdAndMsgQueueMap.get(e.getKey()));
      if (!e.getValue().isEmpty()) {
        System.err.println("Queue is not empty at end of round");
      }

    }
  }

  /**
   * Problem: why are there so many things in neighborSet?
   */
  public void sendExploreMsgToAllNeighbors() {
    for (int targetNodeId : neighborSet) {
      if (targetNodeId == myId) {
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
      sleep();
      System.err.println(e.getValue().size() + " " + e.getKey() + " " + e.getValue());
      sleep();
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
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void setName(String name) {
    this.name = name;
  }

  public void fillLocalMessagesToSend() {
    // System.err.println("Filling localMessagesToSend.");
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
      localMessagesToSend.get(masterNode.getId()).put(new Message(myId, round, myMaxUid, "Done"));

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public int getMyParent() {
    return myParent;
  }

  public HashSet<Integer> getNeighborSet() {
    return neighborSet;
  }

  public HashSet<Integer> getNackReceived() {
    return nackReceived;
  }

  public HashSet<Integer> getAckReceived() {
    return ackReceived;
  }

}