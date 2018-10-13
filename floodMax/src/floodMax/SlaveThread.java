package floodMax;

import message.*;
import java.util.Set;
import java.util.Queue;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Iterator;

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
  protected Set<Integer> neighbours = new HashSet<Integer>();

  protected ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> localMessagesToSend = new ConcurrentHashMap<>();
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
  }

  public void processRoundNumberMessage(Message msg) {
    this.round = msg.getRound();
    if (this.round == 0) {
      // send explore message to all the neighbors
      newInfo = true;
    }

    else {
      // send messages intended for this round
      // Just swallow this msg. Because Runnable can be re-run multiple times.
      // unless the vertex is a thread and can't be started twice.
    }
  }

  public synchronized void ran() {

    // check for message in hashmap queue
    // get hashmap priority queue in a temp queue
    // run until termination condition is encountered

    while (!terminated) {

      // drain messages from global queue.
      LinkedBlockingQueue<Message> temporaryLocalMessageQueue = fetchFromGlobalQueue();

      // process messages and sending messages.
      // newInfo = false;
      while (!temporaryLocalMessageQueue.isEmpty()) {
        Message msg = temporaryLocalMessageQueue.poll();

        if (msg.getmType().equals("Terminate")) {
          processTerminateMessage();
          break;
        } else if (msg.getmType().equals("Round_Number")) {
          processRoundNumberMessage(msg);

        } else if (msg.getRound() == this.round) {

          // process Explore msg (increment round number inside)
          if (msg.getmType().equals("Explore")) {
            processExploreMsg(msg);
          }

        }
      }
      // after done processing incoming messages, send messages for next round
      // send explore messages to all neighbors except parent
      if (newInfo) {
        int neighbour_id;
        for (int i = 0; i < neighbours.size(); i++) {
          neighbour_id = neighbours.get(i);
          
          //avoid parent
          if (neighbour_id != this.myParent) {
            Message temp_msg = new Message(this.nodeIndex, this.round + 1, this.myMaxUid, "Explore");
            tempMsgPair = new DestinationAndMsgPair(neighbour_id, temp_msg);
            msgPairsToSend.add(tempMsgPair);

          }
        }
      }
      // else divide message from the string into arraylist and send msgs to
      // corresponding nodes
      else {

      }

      // Message to master about Round Completion
      System.out.println("send round done message to master by thread " + this.nodeIndex);
      Message messageToMaster = new Message(this.id, 0, this.round, "Done");
      temp_msg_pbq = masterNode.globalIdAndMsgQueueMap.get(0);
      temp_msg_pbq.add(messageToMaster);
      masterNode.globalIdAndMsgQueueMap.put(0, temp_msg_pbq);
      newInfo = false;
    }

  } // end of terminate

  // end of run

  public void run() {
    System.out.println("I RAN!!!" + id + " round " + round);

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

    // drain messages from global queue.
    LinkedBlockingQueue<Message> temporaryLocalMessageQueue = fetchFromGlobalQueue();

    // process messages and sending messages.
    while (!temporaryLocalMessageQueue.isEmpty()) {
      Message msg = temporaryLocalMessageQueue.poll();

      if (msg.getmType().equals("Terminate")) {
        processTerminateMessage();
        break;
      } else if (msg.getmType().equals("Round_Number")) {
        processRoundNumberMessage(msg);

      } else if (msg.getRound() == this.round) {

        // process Explore msg (increment round number inside)
        if (msg.getmType().equals("Explore")) {
          try {
            processExploreMsg(msg);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }

      }
    }

    // send round completion message to master

    // set the queue to global queue
    pushToGlobalQueue();
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
    if (nackCount == neighbours.size() - 1) {
      // leaf node, send to parent.
      return new Message(id, round + 1, myMaxUid, "ACK");
    } else if (ackCount == neighbours.size()) {
      // leader node. Send leader message to master.
      return new Message(id, round + 1, myMaxUid, "Leader");
    } else if (nackCount + ackCount == neighbours.size() - 1) {
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
  public synchronized LinkedBlockingQueue<Message> fetchFromGlobalQueue() {
    LinkedBlockingQueue<Message> localQ = new LinkedBlockingQueue<>();
    if (!globalIdAndMsgQueueMap.get(id).isEmpty()) {
      globalIdAndMsgQueueMap.get(id).drainTo(localQ);
    }

    if (!globalIdAndMsgQueueMap.get(id).isEmpty()) {
      System.err.println("Global queue is not empty. We have a prolem.");
    }
    return localQ;
  }

  /**
   * Drain the local msg queue and put all in the global queue. The local queue
   * should be empty after this. This should be done at the end of each round.
   * e.g. each thread has a ConcurrentHashMap similar to the global one, but
   * should be empty. At end of each round, it unloads the queues to the global
   * ConcurrentHashMap.
   */
  public synchronized void pushToGlobalQueue() {
    for (Entry<Integer, LinkedBlockingQueue<Message>> e : localMessagesToSend.entrySet()) {
      e.getValue().drainTo(globalIdAndMsgQueueMap.get(e.getKey()));
      if (!e.getValue().isEmpty()) {
        System.err.println("Queue is not empty at end of round");
      }
    }
  }

  public void setNeighbours(Set<Integer> neighbours) {
    this.neighbours = neighbours;
  }

  public void insertNeighbour(int neighborId) {
    this.neighbours.add(neighborId);
  }
}