package floodMax;

import message.*;
import java.util.Set;
import java.util.Queue;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Iterator;

public class SlaveThread implements Runnable {

  protected int id;
  private int nodeIndex;
  private MasterThread masterNode;
  private int parent = -1;
  int maxUid;
  ArrayList<Integer> neighbours;
  protected boolean newInfo;
  protected String messageString;
  protected int round;
  protected Message tempMsg;
  private int nackCount;
  private int actCount;
  public ConcurrentHashMap<Integer, ArrayList<Integer>> slave_children;
  protected LinkedBlockingQueue<Message> localMessageQueue;
  protected LinkedBlockingQueue<Message> temp_pq;
  private ArrayList<Integer> indexOfChildren;
  static boolean terminated;
  private LinkedBlockingQueue<Message> temp_msg_pbq;
  protected Map<Integer, SlaveThread> neighbors;
  // protected Queue<Message> nextRoundMsg = new LinkedBlockingQueue<Message>();
  // protected Queue<Message> thisRoundMsg = new LinkedBlockingQueue<Message>();
  private ArrayList<DestinationAndMsgPair> msgPairsToSend;
  private DestinationAndMsgPair tempMsgPair;
  private Queue<Integer> finalOutput;
  // Empty constructor for subclass.
  private int current_node;
  private static ConcurrentHashMap<Integer, Integer> indexToIdMapping;

  public SlaveThread() {
  }

  /**
   * Constructor.
   * 
   * @param id
   * @param masterNode
   * @param nodeIndex
   *          = row in the matrix
   */

  public SlaveThread(int id, MasterThread masterNode, int nodeIndex,
      ConcurrentHashMap<Integer, ArrayList<Integer>> children, ConcurrentHashMap<Integer, Integer> Sno_id_mapping) {
    this.id = id;
    this.maxUid = id;
    this.masterNode = masterNode;
    this.nodeIndex = nodeIndex;
    temp_pq = new LinkedBlockingQueue<>();
    this.newInfo = true;
    neighbours = new ArrayList<>();
    this.round = 0;
    this.nackCount = 0;
    this.actCount = 0;
    this.slave_children = children;
    slave_children = new ConcurrentHashMap<>();
    indexOfChildren = new ArrayList<>();
    temp_msg_pbq = new LinkedBlockingQueue<Message>();
    msgPairsToSend = new ArrayList<>();
    finalOutput = new LinkedList<Integer>();
    SlaveThread.indexToIdMapping = Sno_id_mapping;
    SlaveThread.terminated = false;
    neighbors = new ConcurrentHashMap<Integer, SlaveThread>();
  }

  public void processTerminateMessage() {
    System.out.println("Leader id: " + maxUid);
    // Obtaining an iterator for the entry set
    Iterator<Map.Entry<Integer, Integer>> it = indexToIdMapping.entrySet().iterator();

    // output the graph and stop execution
    current_node = this.id;
    finalOutput.add(current_node);
    System.out.print("The tree formed is " + current_node + "-->");
    while (!(finalOutput.isEmpty())) {
      current_node = finalOutput.poll();

      // Iterate through HashMap entries(Key-Value pairs)
      while (it.hasNext()) {

        Map.Entry<Integer, Integer> m_e = it.next();
        int val = m_e.getValue();
        if (val == current_node) {
          indexOfChildren = slave_children.get(m_e.getKey());
        }
      }
      while (!(indexOfChildren.isEmpty())) {
        finalOutput.add(indexOfChildren.get(0));
        indexOfChildren.remove(0);
      }
      System.out.print(current_node + "-->");
    }
    terminated = true;
  }

  public void processRoundNumberMessage() {
    this.round = tempMsg.getRound();
    System.out
        .println("Inside else of Round Number, round number is " + this.round + " Sno thread is " + this.nodeIndex);
    if (this.round == 0) {
      // send explore message to all the neighbors
      newInfo = true;
    }

    else {
      // send messages intended for this round
      for (int i = 0; i < msgPairsToSend.size(); i++) {
        DestinationAndMsgPair messagePair = new DestinationAndMsgPair();
        messagePair = msgPairsToSend.get(i);
        temp_msg_pbq = masterNode.globalIdAndMsgQueueMap.get(messagePair.GetId());
        temp_msg_pbq.add(messagePair.GetMsg());
        masterNode.globalIdAndMsgQueueMap.put(messagePair.GetId(), temp_msg_pbq);
      }
    }
    // make thread sleep for 2 seconds; local queue should be empty here
    for (int i = 0; i < 200000; i++)
      ;
  }

  public synchronized void run() {
    System.out.println("I RAN!!!" + this.nodeIndex);

    masterNode.globalIdAndMsgQueueMap.put(this.nodeIndex, temp_pq);
    // find neighbors and store in neighbour Array list
    for (int temp = 1; temp < masterNode.size; temp++) {
      // if edge exist in the graph provided
      if (masterNode.matrix[this.nodeIndex][temp] == 1) {
        neighbours.add(temp);
      }
    }
    System.out.println("Neighbors for " + this.nodeIndex + " : " + neighbours.size());
    // check for message in hashmap queue
    // get hashmap priority queue in a temp queue
    // run until termination condition is encountered

    while (!terminated) {

      localMessageQueue = masterNode.globalIdAndMsgQueueMap.get(this.nodeIndex);

      // newInfo = false;
      while (!localMessageQueue.isEmpty()) {
        // System.out.println("Size of queue is " + localMessageQueue.size() + "Index of
        // thread is " + this.nodeIndex);
        tempMsg = localMessageQueue.poll();
        System.out.println("Size of queue is " + localMessageQueue.size() + "Sno thread is " + this.nodeIndex
            + " Message type is " + tempMsg.getmType());

        // deal with different message types

        if (tempMsg.getmType().equals("Terminate")) {
          processTerminateMessage();
          break;
        } else if (tempMsg.getmType().equals("Round_Number")) {
          processRoundNumberMessage();
        } else if (tempMsg.getRound() == this.round) {
          System.out.println("If message recieved in this round;inside 3 else");
          // increments the round number
          if (tempMsg.getmType().equals("Explore")) {
            System.out.println("Inside explore");
            if (this.maxUid > tempMsg.getmaxUID()) {
              this.maxUid = tempMsg.getmaxUID();
              this.parent = tempMsg.getSenderId();
              newInfo = true;
            } else if (this.maxUid == tempMsg.getmaxUID()) {

              // check which parent node has bigger id and choose a parent
              if (this.parent > tempMsg.getSenderId()) {
                Message temp_msg = new Message(this.nodeIndex, this.round + 1, this.maxUid, "N_ACK");
                tempMsgPair = new DestinationAndMsgPair(tempMsg.getSenderId(), temp_msg);
                msgPairsToSend.add(tempMsgPair);

              } else {
                Message temp_msg = new Message(this.nodeIndex, this.round + 1, this.maxUid, "N_ACK");
                tempMsgPair = new DestinationAndMsgPair(this.parent, temp_msg);
                msgPairsToSend.add(tempMsgPair);

                this.parent = tempMsg.getSenderId();

              }
            }
          }

          // count number of ACK and N_ACK
          else if (tempMsg.getmType().equals("N_ACK")) {
            System.out.println(this.id + " got N_ACK");
            this.nackCount++;
          } else if (tempMsg.getmType().equals("ACK")) {
            this.actCount++;
            System.out.println(this.id + "got ACK");
            indexOfChildren.add(tempMsg.getSenderId());
            slave_children.put(this.nodeIndex, indexOfChildren);
          }
        }

        // after done processing all the messages, send messages for next round
        // send explore messages to all neighbors except parent
        if (newInfo) {
          System.out.println("Inside new info");
          msgPairsToSend.removeAll(msgPairsToSend);
          int neighbour_id;
          System.out.println(
              "Size of neighbour list is " + neighbours.size() + "index of thread is " + this.nodeIndex + "\n");
          for (int i = 0; i < neighbours.size(); i++) {
            System.out.println("neighbour id is " + neighbours.get(i));
            neighbour_id = neighbours.get(i);
            System.out.println("parent is " + this.parent);
            if (neighbour_id != this.parent) {
              System.out.println("Sending message to neighbor " + neighbour_id + " from " + this.nodeIndex + "\n");
              Message temp_msg = new Message(this.nodeIndex, this.round + 1, this.maxUid, "Explore");
              tempMsgPair = new DestinationAndMsgPair(neighbour_id, temp_msg);
              msgPairsToSend.add(tempMsgPair);

            }
          }
        }
        // else divide message from the string into arraylist and send msgs to
        // corresponding nodes
        else {
          System.out.println("Inside else of new info");
          // for node leaf
          if (nackCount == neighbours.size() - 1) {
            Message temp_msg = new Message(this.nodeIndex, this.round + 1, this.maxUid, "ACK");
            tempMsgPair = new DestinationAndMsgPair(this.parent, temp_msg);
            msgPairsToSend.add(tempMsgPair);
          }
          // leader receives only ACT, and has no parent.
          else if (actCount == neighbours.size()) {
            Message temp_msg = new Message(this.nodeIndex, this.round + 1, this.maxUid, "Leader");
            tempMsgPair = new DestinationAndMsgPair(0, temp_msg);
            // send msg to my local queue, then to master
            msgPairsToSend.add(tempMsgPair);

          }
          // for internal nodes, NACK + ACT = neighbor size -1
          else if (nackCount + actCount == neighbours.size() - 1) {
            Message temp_msg = new Message(this.nodeIndex, this.round + 1, this.maxUid, "ACK");
            // send message to my local queue, then to master
            tempMsgPair = new DestinationAndMsgPair(this.parent, temp_msg);
            msgPairsToSend.add(tempMsgPair);
          }

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

  }// end of run

  /**
   * Pair of destination thread <index, Message>
   * 
   * @author khoa
   *
   */
  class DestinationAndMsgPair {
    int indexOfThread;
    Message msg;

    public DestinationAndMsgPair() {
    }

    public DestinationAndMsgPair(int indexOfThread, Message msg) {
      this.indexOfThread = indexOfThread;
      this.msg = msg;
    }

    public int GetId() {
      return indexOfThread;
    }

    public Message GetMsg() {
      return msg;
    }

  };
}