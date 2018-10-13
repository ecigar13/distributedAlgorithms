package floodMax;

import message.*;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import java.util.HashSet;

public class SlaveThread implements Runnable {
  protected String name;
  protected static boolean terminated;
  protected static ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> globalIdAndMsgQueueMap;

  protected int id;
  protected int myMaxUid;
  protected int round;
  private MasterThread masterNode;

  private int myParent = -1;
  protected Set<Integer> children = new HashSet<Integer>();
  private Set<Integer> neighborSet = new HashSet<Integer>();

  protected ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> localMessagesToSend = new ConcurrentHashMap<>();
  protected LinkedBlockingQueue<Message> localMessageQueue = new LinkedBlockingQueue<>();
  protected boolean newInfo;
  protected String messageString;

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
    this.id = id;
    this.myMaxUid = id;
    this.masterNode = masterNode;
    this.newInfo = true;
    this.round = 0;
    this.nackCount = 0;
    this.ackCount = 0;

    name = "Thread_" + id;

    SlaveThread.globalIdAndMsgQueueMap = globalIdAndMsgQueueMap;
    SlaveThread.terminated = false;

    fillLocalMessagesToSend();
  }

  public void fillLocalMessagesToSend() {
    System.err.println("Filling localMessagesToSend.");
    localMessagesToSend.put(0, new LinkedBlockingQueue<Message>());
    for (int i : neighborSet) {
      // add signal to start
      localMessageQueue = new LinkedBlockingQueue<Message>();
      localMessagesToSend.put(i, localMessageQueue);
    }
  }

  /**
   * Put round done message to local queue.
   */
  public void sendRoundDoneToMaster() {
    try {
      localMessagesToSend.get(masterNode.getId()).put(new Message(id, this.round + 1, this.myMaxUid, "Explore"));

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Process message types: Terminate, Round_Number, Explore.
   */
  public void processMessageTypes() {
    while (!localMessageQueue.isEmpty()) {
      Message msg = localMessageQueue.poll();

      if (msg.getmType().equalsIgnoreCase("Terminate")) {
        processTerminateMessage();
        break;

      } else if (msg.getmType().equalsIgnoreCase("Round_Number")) {
        processRoundNumberMessage(msg);

      } else if (msg.getmType().equalsIgnoreCase("Explore")) {

        // process Explore msg (increment round number inside)
        try {
          processExploreMsg(msg);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

  public void run() {
    sleep();
    System.out.println(name + " round " + round + " leader " + myMaxUid);

    // base case, check Nack and Ack count first.
    Message temp = countNackAck();

    // if the message is "Leader", put message in queue for leader and stop thread
    // immediately.
    try {
      if (temp != null && temp.getmType() == "Leader") {
        localMessagesToSend.get(0).put(temp);
        this.wait();
        pushToGlobalQueue();
        return;
      } else if (temp != null) {
        // if message is anything else but leader, send message to parent queue. This
        // must be an ACK msg.
        localMessagesToSend.get(myParent).put(temp);
      }
      // if Nack, Ack is not full yet, return null. Therefore, do nothing.

    } catch (Exception e) {
      e.printStackTrace();
    }

    fetchFromGlobalQueue(localMessageQueue);

    // process messages and sending messages.
    processMessageTypes();

    // after done processing incoming messages, send messages for next round
    // send explore messages to all neighbors except parent
    sendExploreMsgToAllNeighbors();

    // Message to master about Round Completion
    sendRoundDoneToMaster();
    newInfo = false;
    // set the queue to global queue
    pushToGlobalQueue();

    System.out.println("The thread stops. " + id);

  }

  public void sendExploreMsgToAllNeighbors() {
    for (int n : neighborSet) {
      if (n != myParent) {
        try {
          localMessagesToSend.get(n).put(new Message(id, round + 1, myMaxUid, "Explore"));
        } catch (Exception e) {
          e.getMessage();
        }
      }
    }
  }

  /**
   * Only works with round 0.
   * 
   * @param msg
   */
  public void processRoundNumberMessage(Message msg) {
    round = msg.getRound();
    if (round == 0) {
      sendExploreMsgToAllNeighbors();
      newInfo = false;
    }

    else {
      // send messages intended for this round
      // Just swallow this msg. Because Runnable can be re-run multiple times.
      // unless the vertex is a thread and can't be started twice.
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
      if (this.myParent > msg.getSenderId()) {
        // send nack to sender.
        Message nackMsg = new Message(id, round + 1, myMaxUid, "N_ACK");
        localMessagesToSend.get(msg.getSenderId()).put(nackMsg);

      } else if (myParent < msg.getSenderId()) {
        // pick a new parent: send nack to parent, set the msg sender as parent.
        Message nackMsg = new Message(id, round + 1, myMaxUid, "N_ACK");
        localMessagesToSend.get(myParent).put(nackMsg);
        this.myParent = msg.getSenderId();
      }
    }
  }

  /**
   * Check nackCount and ackCount and come up with a message.
   * 
   * @return a message based on the count
   */
  public Message countNackAck() {
    if (nackCount == neighborSet.size() - 1) {
      // leaf node, send to parent.
      return new Message(id, round + 1, myMaxUid, "ACK");
    } else if (ackCount == neighborSet.size()) {
      // leader node. Send leader message to master.
      return new Message(id, round + 1, myMaxUid, "Leader");
    } else if (nackCount + ackCount == neighborSet.size() - 1) {
      // internal node, send to parent.
      return new Message(id, round + 1, myMaxUid, "ACK");
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
   * Fetch the queue of this thread from the global queue. The Global queue will
   * have zero element.
   * 
   * @return
   */
  public synchronized void fetchFromGlobalQueue(LinkedBlockingQueue<Message> localQ) {
    if (!globalIdAndMsgQueueMap.get(id).isEmpty()) {
      globalIdAndMsgQueueMap.get(id).drainTo(localQ);
    }

    System.err.println("Fetching from global queue. " + globalIdAndMsgQueueMap.get(id).size() + " " + localQ.size());
    if (!globalIdAndMsgQueueMap.get(id).isEmpty()) {
      System.err.println("Global queue is not empty. We have a prolem.");
    }
  }

  /**
   * Drain the local msg queue and put all in the global queue. The local queue
   * should be empty after this. This should be done at the end of each round.
   * e.g. each thread has a ConcurrentHashMap similar to the global one, but
   * should be empty. At end of each round, it unloads the queues to the global
   * ConcurrentHashMap.
   */
  public synchronized void pushToGlobalQueue() {
    System.err.println("Pushing to global queue.");
    for (Entry<Integer, LinkedBlockingQueue<Message>> e : localMessagesToSend.entrySet()) {
      e.getValue().drainTo(globalIdAndMsgQueueMap.get(e.getKey()));
      if (!e.getValue().isEmpty()) {
        System.err.println("Queue is not empty at end of round");
      }
    }
  }

  public void setNeighbours(Set<Integer> neighbours) {
    this.neighborSet = neighbours;
  }

  public void insertNeighbour(int neighborId) {
    this.neighborSet.add(neighborId);
  }

  public int getId() {
    return id;
  }

  public void sleep() {
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}