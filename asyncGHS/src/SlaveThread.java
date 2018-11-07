
import java.util.HashSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.sun.jmx.snmp.Timestamp;
import com.sun.org.apache.xalan.internal.xsltc.compiler.Template;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import sun.reflect.generics.tree.Tree;

public class SlaveThread implements Runnable {
  protected String name;
  protected boolean isLeader = false;
  protected boolean terminated;

  protected int id;
  protected int componentId;
  protected int round;
  private int myParent;
  protected double mwoe;
  protected MasterThread masterNode;
  protected int level = 0;
  boolean mwoeFound = false;

  protected Message currentReportMessage;

  protected ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> globalIdAndMsgQueueMap;
  protected ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> localMessagesToSend;
  protected LinkedBlockingQueue<Message> localMessageQueue = new LinkedBlockingQueue<>();

  protected HashSet<Integer> connected = new HashSet<>();

  protected TreeSet<Integer> reportReceived = new TreeSet<>();
  protected ArrayList<Integer> waitingForResponse = new ArrayList<>();

  protected TreeSet<Link> basicEdge = new TreeSet<>(new CompareLinks());
  protected TreeSet<Link> branch = new TreeSet<>(new CompareLinks());
  protected TreeSet<Link> rejected = new TreeSet<>(new CompareLinks());

  protected Random r = new Random();

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
    this.componentId = id;
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
      waitingForResponse.remove(m.getSenderId());

      if (m.getmType().equals("Round_Number")) {
        processRoundNumber(m);// done
      } else if (m.getmType().equals("initiate")) {
        processInitMessage(m);
      } else if (m.getmType().equals("test")) {
        processTestMsg(m);
      } else if (m.getmType().equals("report")) {
        processReportMessage(m);// not done
      } else if (m.getmType().equals("accept")) {
        processAcceptMessage(m);
      } else if (m.getmType().equals("reject")) {
        processRejectMsg(m);
      } else if (m.getmType().equals("changeRoot")) {
        // what to do here?
      } else if (m.getmType().equals("connect")) {
        processConnectMessage(m);
      } else if (m.getmType().equals("levelMismatched")) {
        processLevelMismatchedMsg(m);
      }
    }
  }

  /**
   * 
   * @param m
   * @throws InterruptedException
   */
  public void processLevelMismatchedMsg(Message m) throws InterruptedException {
    for (Link l : basicEdge) {
      if (l.getWeight() > m.getMwoe()) {
        sendTestToSmallestBasic();
      }
    }
  }

  /**
   * Add the edge to rejected set. Remove from basic edge.Send test msg to the
   * smallest basic edge.
   * 
   * @param m
   */
  public void processRejectMsg(Message m) throws InterruptedException {
    for (Link l : basicEdge) {
      if (l.getTo() == m.getSenderId()) {
        rejected.add(l);
        basicEdge.remove(l);

      }
    }
    sendTestToSmallestBasic();
  }

  /**
   * Remove the sender from waiting for response (because I got the response).
   * Then if I'm not the leader, send a "report" msg to parent.
   * 
   * @param m
   */
  public void processAcceptMessage(Message m) {
    waitingForResponse.remove(m.getSenderId());
    if (componentId != id) {
      Message temp = new Message(id, myParent, m.getMwoe(), level, r.nextInt(19) + 1, componentId, "report");
      temp.getPath().add(m.getSenderId());
      temp.getPath().add(id);
    } else {
      // I'm the leader, I store the message.
      currentReportMessage = m;
    }
  }

  /*
   * if(component ID = my id) 3.1 Check waiting_for_response queue 3.2 if( empty )
   * 3.2.1 check if mwoe_found = true 3.2.1.1 send "MOWE_FOUND" message to the
   * MWOE node 3.2.2 else //mwoe_found != true 3.2.1 Leader sends "Initiate"
   * message on the "tree edges" and send "TEST" message to 1 "basic edge. 3.2.2
   * put node ids of all nodes to whom a message is sent into waiting_for_response
   * queue 3.2.3 set mwoe = infinity
   */
  public void leaderDecideMsgToSend() throws InterruptedException {
    if (componentId == id && waitingForResponse.size() == 0) {
      if (mwoeFound) {
        Message temp = new Message(id, currentReportMessage.getSenderId(), 0, level, r.nextInt(19) + 1, componentId,
            "mwoeFound");
        localMessagesToSend.get(currentReportMessage.getSenderId()).put(temp);
      } else {
        sendInitiateToBranch();
        sendTestToSmallestBasic();
        // these already added ids of all threads who I sent msg to.
        mwoe = Double.MAX_VALUE;
      }
    }
  }

  /**
   * set my round to the round master tells me.
   * 
   * @param m
   */
  public void processRoundNumber(Message m) {
    round = m.getRound();
  }

  /**
   * Go through messages in my queue and reduce the round by 1. Not less than 0.
   * Call this at every round.
   */
  public void reduceRoundInMsg() {
    for (Entry<Integer, LinkedBlockingQueue<Message>> e : localMessagesToSend.entrySet()) {
      for (Message message : e.getValue()) {
        int temp = message.getRound() - 1 >= 0 ? message.getRound() - 1 : 0;
        message.setRound(temp);
      }
    }
  }

  /**
   * Send initiate to all branches and send test to smallest basic edge.
   * 
   * @param m
   */
  public void processInitMessage(Message m) throws InterruptedException {
    sendTestToSmallestBasic();
    sendInitiateToBranch();
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
      for (int i : basicEdge) {
        if (mwoe > basicEdge.get(i)) {
          tempNeighbor = i;
          mwoe = basicEdge.get(i);
        }
      }

      if (isLeader) {
        broadcastConnect(m, tempNeighbor);
      } else if (tempNeighbor != id) {
        // if basic branches have less weight than mwoe
        Message temp = new Message(id, null, mwoe, null, round, componentId, "report");
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

  public void absorb() {

  }

  /**
   * If I'm the mwoe node, check if I sent connect msg to the sender. Then merge
   * or absorb. If I'm the leaf node (path size is 1), record that I'm sent
   * connect message. Then send it. Else just send it.
   * 
   */
  public void processConnectMessage(Message m) throws InterruptedException, NullPointerException {
    if (m.getPath().size() == 0) {

      // if I sent connect msg before. Then merge or absorb.
      if (connected.contains(m.getSenderId())) {
        merge(m);
        connected.remove(m.getSenderId());
      } else if (level > m.getLevel()) {
        absorb();
        connected.remove(m.getSenderId());
      }
    } else if (m.getPath().size() == 1) {
      connected.add(m.getReceiverId());

      // then send it.
      int temp = m.getPath().removeLast();
      m.setSenderId(id);
      localMessagesToSend.get(temp).put(m);
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
    Message temp = new Message(id, null, mwoe, null, round, componentId, "connect");
    if (basicEdge.contains(id)) {
      localMessagesToSend.get(id).put(temp);
    }
    m.setMwoe(mwoe);
    m.setmType("connect");
    m.setSenderId(id);

    localMessagesToSend.get(m.getPath().removeLast()).put(m);

  }

  /**
   * Process Test messages. If my lvl is > your lvl,
   * 
   * @param m
   */
  public void processTestMsg(Message m) throws InterruptedException {
    if (level >= m.getLevel()) {
      if (componentId == m.getComponentId()) {
        localMessagesToSend.get(m.getSenderId())
            .put(new Message(id, m.getSenderId(), m.getMwoe(), level, r.nextInt(19) + 1, componentId, "reject"));

      } else if (componentId != m.getComponentId()) {
        localMessagesToSend.get(m.getSenderId())
            .put(new Message(id, m.getSenderId(), m.getMwoe(), level, r.nextInt(19) + 1, componentId, "accept"));
      }
    } else {
      localMessagesToSend.get(m.getSenderId())
          .put(new Message(id, m.getSenderId(), m.getMwoe(), level, r.nextInt(19) + 1, componentId, "levelMismatched"));
    }
  }

  /**
   * Implement. Merge is done by both child and parent.
   */
  public void merge(Message m) {
    if (sentConnect.contains(m.getSenderId()) && receivedConnect.contains(m.getSenderId())) {
      if (componentId < m.getComponentId()) {
        componentId = m.getComponentId();
        myParent = m.getSenderId();
      }
      branch.add(m.getSenderId());
      basicEdge.remove(m.getSenderId());
      // broadcast change root to the rest of the tree, change their maxUid to this
      // maxUid.
    }
  }

  /**
   * Send initiate to all branches except parent.
   */
  public void sendInitiateToBranch() throws InterruptedException {
    for (Link e : branch) {
      if (e.getFrom() == myParent) {
        continue;
      }
      if (!waitingForResponse.contains(e.getTo())) {
        System.out.println("Send initiate to " + e.getTo());
        localMessagesToSend.get(e.getTo())
            .put(new Message(id, e.getTo(), e.getWeight(), level, r.nextInt(19) + 1, componentId, "initiate"));
        waitingForResponse.add(e.getTo());
      }
    }
  }

  /**
   * Send test message to the smallest edge in basic edges.
   * 
   * @throws InterruptedException
   */
  public void sendTestToSmallestBasic() throws InterruptedException {
    Link e = basicEdge.first();
    if (!waitingForResponse.contains(e.getTo())) {
      System.out.println("Send test to " + e.getTo());
      localMessagesToSend.get(e.getTo())
          .put(new Message(id, e.getTo(), e.getWeight(), r.nextInt(19) + 1, round, componentId, "test"));
      waitingForResponse.add(e.getTo());
    }
  }

  /**
   * Need to implement this. Don't use while loop.
   */
  public void run() {
    processMessageTypes();
    checkMwoeFound();
    reduceRoundInMsg();
    goThroughMsgQueueAndSendMsgRoundZero();
    // System.out.println(name + " start. round " + round + " leader " + componentId
    // + " parent " + myParent + " "
    // + branch.size() + " " + rejected.size() + " " + basicEdge.size());
    // try {
    // if (!terminated) {
    // fetchFromGlobalQueue(localMessageQueue);
    // processMessageTypes();
    //
    // // after done processing incoming messages, send msg for next round
    // sendInitiateToBranch();
    // if (round != 0)
    // sendRoundDoneToMaster();
    // // then master will drain to its own queue.
    //
    // }
    // } catch (Exception e) {
    // e.printStackTrace();
    // }
    // System.out.println(name + " stop. round " + round + " leader " + componentId
    // + " parent " + myParent + " "
    // + branch.size() + " " + rejected.size() + " " + basicEdge.size());
    // System.out.println(branch.toString());
    // System.out.println(rejected.toString());
    // System.out.println(basicEdge.toString());
    // System.out.println();

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
    System.out.println("Thread terminated. Leader id: " + componentId);
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

  public void insertNeighbour(Link l) {
    this.basicEdge.add(l);
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
    for (Entry<Integer, Double> e : basicEdge.entrySet()) {
      localMessagesToSend.put(e.getKey(), new LinkedBlockingQueue<Message>());
    }
  }

  /**
   * Put round done message to local queue.
   */
  public void sendRoundDoneToMaster() {
    try {
      localMessagesToSend.get(masterNode.getId()).put(new Message(id, null, mwoe, null, round, componentId, "Done"));

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
    return basicEdge;
  }

}