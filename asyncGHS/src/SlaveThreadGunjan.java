import java.util.HashSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;

import com.sun.jmx.snmp.Timestamp;
import com.sun.org.apache.bcel.internal.generic.IF_ACMPEQ;
import com.sun.org.apache.xalan.internal.xsltc.compiler.Template;

import javafx.scene.Parent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import sun.reflect.generics.tree.Tree;

public class SlaveThreadGunjan implements Runnable {
  protected String name;
  protected boolean isLeader = false;
  protected boolean terminated;
  protected boolean Leader;
  protected int id;
  protected String componentId;
  protected int round;
  private int myParent;
  protected double mwoe;
  protected MasterThread masterNode;
  protected int level = 0;
  boolean mwoeFound = false;

  protected Message currentReportMessage;
  protected Message currentTestMsg;

  protected ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> globalIdAndMsgQueueMap;
  protected ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> localMsgToReduce;
  protected ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> localMsgToSend;
  protected LinkedBlockingQueue<Message> localMessageQueue = new LinkedBlockingQueue<>();

  protected HashSet<Integer> connected = new HashSet<>();

  protected TreeSet<Integer> reportReceived = new TreeSet<>();
  protected ArrayList<Integer> waitingForResponse = new ArrayList<>();

  protected TreeSet<Link> basicEdge = new TreeSet<>(new CompareLinks());
  protected TreeSet<Link> branch = new TreeSet<>(new CompareLinks());
  protected TreeSet<Link> rejected = new TreeSet<>(new CompareLinks());

  protected Random r = new Random();

  /**
   * Need to implement this. Don't use while loop.
   */
  public void run() {
    try {
      fetchFromGlobalQueue(localMessageQueue);
      // if I'm leader, send initiate.
      processMessageTypes();
      decideToSendReportMsg();
      // send msg I need to send into local queue.
      reduceRoundInMsg();
      drainToGlobalQueue();
      sendRoundDoneToMaster();

    } catch (Exception e) {
      e.printStackTrace();
    }

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
        processInitMessage(m); // done
      } else if (m.getmType().equals("test")) {
        processTestMsg(m); // done
      } else if (m.getmType().equals("report")) {
        processReportMessage(m); // done
      } else if (m.getmType().equals("accept")) {
        processAcceptMessage(m); // done
      } else if (m.getmType().equals("reject")) {
        processRejectMsg(m);
      } else if (m.getmType().equals("changeRoot")) {
        // changeRoot is essentially connect msg
        processConnectMessage(m);
      } else if (m.getmType().equals("connect")) {
        processConnectMessage(m);
      } else if (m.getmType().equals("levelMismatched")) {
        processLevelMismatchedMsg(m);
      } else if (m.getmType().equals("New_Component")) {
        processNewComponent(m);
      }
    }
  }

  /**
   * If level is mismatched, send test to the next one.
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
   * Save the test msg.
   * 
   * @param m
   */
  public void processAcceptMessage(Message m) throws InterruptedException {
    waitingForResponse.remove(m.getSenderId());
    // just save the msg.
    currentTestMsg = m;
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
    if (Leader == true && waitingForResponse.size() == 0) {
      if (mwoeFound) {
        Message temp = new Message(id, currentReportMessage.getSenderId(), 0, level, r.nextInt(19) + 1, componentId,
            "mwoeFound");
        localMsgToReduce.get(currentReportMessage.getSenderId()).put(temp);
      } else {
        sendInitiateToBranch();
        sendTestToSmallestBasic();
        // these already added ids of all threads who I sent msg to.
        mwoe = Double.MAX_VALUE;
      }
    }
  }

  public void decideToSendReportMsg() throws InterruptedException {
    if (reportReceived.size() == branch.size() && currentReportMessage != null && currentTestMsg != null) {

      // If I got report from all branches, Compare it with what I get from reports.
      Message msgToUse;
      if (currentReportMessage.getMwoe() > currentTestMsg.getMwoe()) {
        msgToUse = currentTestMsg;
      } else {
        msgToUse = currentReportMessage;
      }

      if (isLeader) {
        // if leader, process the message and broadcast back.
        // what if my mwoe is in leader's basic edges???
        broadcastConnect(msgToUse);
      } else {
        // if I pick mwoe from basic edge, construct new msg and send up.
        // also add the last node.

        msgToUse.setSenderId(id);
        msgToUse.getPath().add(id);
        localMsgToReduce.get(myParent).put(msgToUse);
      }

      // clear my set of report and set mwoe to maximum.
      reportReceived.clear();
      currentReportMessage = null;
      currentTestMsg = null;
      mwoe = Double.MAX_VALUE;
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
    // save the msg with smallest mwoe. Discard the rest.
    if (mwoe > m.getMwoe()) {
      mwoe = m.getMwoe();
      reportReceived.add(m.getSenderId());
      currentReportMessage = m;
    }

    // If I got report from all branches, go through basic edges and find the mwoe.
    // Compare it with what I get from reports.
    if (reportReceived.size() == branch.size()) {
      int tempNeighbor = id;

      // compare mwoe with smallest of basic edge.
      if (!basicEdge.isEmpty() && mwoe > basicEdge.first().getWeight()) {
        tempNeighbor = basicEdge.first().getTo();
        mwoe = basicEdge.first().getWeight();
        currentReportMessage = null; // if I pick a basic edge, then discard report msg.
      }

      if (isLeader) {
        // if leader, process the message and broadcast back.
        // what if my mwoe is in leader's basic edges???
        broadcastConnect(m, tempNeighbor);
      } else if (tempNeighbor != id) {
        // if I pick mwoe from basic edge, construct new msg and send up.
        Message temp = new Message(id, myParent, mwoe, level, r.nextInt(19) + 1, componentId, "report");
        // also add the last node.
        temp.getPath().add(tempNeighbor);
        temp.getPath().add(id);
        localMsgToReduce.get(myParent).put(temp);
      } else {

        // If this is not the end, just add itself to msg and send it up.
        m.setSenderId(id);
        m.getPath().add(id);
        localMsgToReduce.get(myParent).put(m);
      }

      // if I sent report up or broadcast connect, clear my set of report and set mwoe
      // to maximum.
      reportReceived.clear();
      mwoe = Double.MAX_VALUE;
    }

  }

  /**
   * Only run this method if leader got report msg from all its children.
   * 
   * if currentReportMsg is "accept" then one of my basic edges is an mwoe, send
   * connect.
   * 
   * if currentReportMsg is not null then the mwoe belongs to my children. Send
   * the changeRoot message back down the path. This is changeRoot message. Near
   * the end of the path, this changeRoot becomes connect message.
   * 
   * @param m
   * @param id
   * @throws InterruptedException
   */
  public void broadcastConnect(Message m) throws InterruptedException {
    // reuse the message.
    m.setSenderId(id);
    m.setMwoe(mwoe);
    m.setLevel(level);
    m.setRound(r.nextInt(19) + 1);
    m.getPath().clear(); // clear the path because mwoe is my neighbor.

    if (currentReportMessage.getmType().equals("accept")) { // one of my basic edges is an mwoe, send connect msg.

      m.setReceiverId(currentReportMessage.getSenderId());
      m.setmType("connect");
      localMsgToReduce.get(m.getReceiverId()).put(m);

    } else {
      // send changeRoot msg down the path.

      m.setmType("changeRoot");
      m.setReceiverId(m.getPath().removeLast());
      localMsgToReduce.get(m.getReceiverId()).put(m);
    }

  }

  /**
   * If I'm the mwoe node, check if I sent connect msg to the sender. Then merge
   * or absorb.
   * 
   * If I'm the leaf node (path size is 1), then this is the changeRoot message. I
   * record that I sent connect message. Then send it. Else just relay it.
   * 
   */
  public void processConnectMessage(Message m) throws InterruptedException, NullPointerException {
    if (m.getPath().size() == 0) {

      m.setmType("connect");

      // if I sent connect msg before. Then merge or absorb.
      if (connected.contains(m.getSenderId())) {
        merge(m);
        connected.remove(m.getSenderId());
      } else if (level > m.getLevel()) {
        absorb(m);
        connected.remove(m.getSenderId());
      }
    } else if (m.getPath().size() == 1) {
      connected.add(m.getReceiverId());

      // then send it.
      int temp = m.getPath().removeLast();
      m.setSenderId(id);
      m.setmType("connect"); // in case this is changeRoot msg.
      localMsgToReduce.get(temp).put(m);
    } else {
      // I'm in the same component, keep sending it.
      int temp = m.getPath().removeLast();
      m.setSenderId(id);
      localMsgToReduce.get(temp).put(m);
    }

  }

  /**
   * Process Test messages. If my lvl is > your lvl.
   * 
   * "accept" message will be used to construct report msg, so always add the
   * path.
   * 
   * @param m
   */
  public void processTestMsg(Message m) throws InterruptedException {
    if (level >= m.getLevel()) {
      if (componentId == m.getComponentId()) {
        localMsgToReduce.get(m.getSenderId())
            .put(new Message(id, m.getSenderId(), m.getMwoe(), level, r.nextInt(19) + 1, componentId, "reject", null));

      } else if (componentId != m.getComponentId()) {
        Message temp = new Message(id, m.getSenderId(), m.getMwoe(), level, r.nextInt(19) + 1, componentId, "accept",
            null);
        temp.getPath().add(id); // important step in deciding to send report message.
        localMsgToReduce.get(m.getSenderId()).put(temp);
      }
    } else {
      localMsgToReduce.get(m.getSenderId())
          .put(new Message(id, m.getSenderId(), m.getMwoe(), level, r.nextInt(19) + 1, componentId, "levelMismatched"));
    }
  }

  /**
   * Implement. Merge is done by both child and parent.
   */
  public void merge(Message m) {
    if (sentConnect.contains(m.getSenderId()) && receivedConnect.contains(m.getSenderId())) {
      if (this.id < m.getSenderId()) {
        Leader = false;
        myParent = m.getSenderId();
      } else {
        myParent = -1;
        componentId = id + " , " + m.getSenderId() + " , " + m.getMwoe();
      }
      level = level + 1;
      branch.add(m.getSenderId());
      basicEdge.remove(m.getSenderId());
      // broadcast change root to the rest of the tree, change their maxUid to this
      // maxUid.
      for (Link i : branch) {
        if (i.getTo() != myParent) {
          Message temp = new Message(id, i.getTo(), mwoe, level, r.nextInt(19) + 1, componentId, "New_Component", null);
        }
      }
    }
  }

  public void absorb(Message m) {
    // put mwoe in tree edge
    // to implement if nt done anywhere else

    // decide who will be the new leader
    if (id < m.getSenderId()) {
      Leader = false;
      myParent = m.getSenderId();
    } else {
      myParent = -1;
    }
    componentId = id + " , " + m.getSenderId() + " , " + m.getMwoe();
    if (this.level > m.getLevel()) {
      level = this.level;
    } else {
      this.level = m.getLevel();
    }

    // Leader sends this new info to everyone in the component i.e to all it tree
    // edges and ask node at other end to forward to all its tree edges
    for (Link i : branch) {
      if (i.getTo() != myParent) {
        Message temp = new Message(id, i.getTo(), mwoe, level, r.nextInt(19) + 1, componentId, "New_Component", null);
      }
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
        localMsgToReduce.get(e.getTo())
            .put(new Message(id, e.getTo(), e.getWeight(), level, r.nextInt(19) + 1, componentId, "initiate", null));
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
      localMsgToReduce.get(e.getTo())
          .put(new Message(id, e.getTo(), e.getWeight(), r.nextInt(19) + 1, round, componentId, "test", null));
      waitingForResponse.add(e.getTo());
    } else {
      System.out.println("Waiting for respond from basic.");
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

  public void setName(String name) {
    this.name = name;
  }

  public int getMyParent() {
    return myParent;
  }

  public TreeSet<Link> getBasicLinks() {
    return basicEdge;
  }

  /**
   * Go through messages in my queue and reduce the round by 1. Not less than 0.
   * Move all msg that are 0 at the front to the localMsgToSend queue.
   * 
   * Maybe implement map reduce.
   */
  public void reduceRoundInMsg() throws InterruptedException {

    for (Entry<Integer, LinkedBlockingQueue<Message>> e : localMsgToReduce.entrySet()) {
      for (Message message : e.getValue()) {
        int temp = message.getRound() - 1 >= 0 ? message.getRound() - 1 : 0;
        message.setRound(temp);
      }

      while (e.getValue().peek().getRound() <= 0) {
        localMsgToSend.get(e.getKey()).put(e.getValue().remove());
      }

    }
  }

  /**
   * Drain the local msg queue and put all in the global queue. Call this at the
   * end of each round. The global queue's structure is similar to this.
   */
  public synchronized void drainToGlobalQueue() {

    for (Entry<Integer, LinkedBlockingQueue<Message>> e : localMsgToSend.entrySet()) {
      e.getValue().drainTo(globalIdAndMsgQueueMap.get(e.getKey()));

      if (!e.getValue().isEmpty()) {
        System.err.println("Queue is not empty at end of round");
      }

    }
  }

  /**
   * Initiate the local queues. Otherwise it will throw nullPointerException.
   */
  public void initLocalMessagesQueues() {
    localMsgToReduce.put(masterNode.getId(), new LinkedBlockingQueue<Message>());
    for (Link l : basicEdge) {
      localMsgToReduce.put(l.getFrom(), new LinkedBlockingQueue<Message>());
    }

    localMsgToSend.put(masterNode.getId(), new LinkedBlockingQueue<Message>());
    for (Link l : basicEdge) {
      localMsgToSend.put(l.getFrom(), new LinkedBlockingQueue<Message>());
    }
    System.err.println("Done initiating SlaveThread queues.");
  }

  /**
   * Put round done message to local queue.
   */
  public void sendRoundDoneToMaster() {
    try {
      localMsgToSend.get(masterNode.getId())
          .put(new Message(id, masterNode.getId(), mwoe, level, round, componentId, "Done"));

    } catch (Exception e) {
      e.printStackTrace();
    }
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

    if (!globalIdAndMsgQueueMap.get(id).isEmpty()) // dont undesrtand this???
    {
      System.err.println("Global queue is not empty. We have a prolem.");
    }
  }

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
    Integer comp_id = new Integer(id);
    this.componentId = comp_id.toString();
    this.myParent = -1;
    this.mwoe = Double.MAX_VALUE;
    this.masterNode = masterNode;
    this.round = 0;
    this.terminated = false;
    // at the start: everyone is a leader
    this.Leader = true;
    name = "Thread_" + id;

    this.globalIdAndMsgQueueMap = globalIdAndMsgQueueMap;
    this.localMsgToReduce = new ConcurrentHashMap<>();
    this.localMsgToSend = new ConcurrentHashMap<>();
    // init local messages to send will be done after construction in MasterThread
    // because Links are added after construction.
  }

  /**
   * set my round to the round master tells me. Will not confuse with delayRound
   * in message.
   * 
   * @param m
   */
  public void processRoundNumber(Message m) {
    round = m.getRound();
    try {
      leaderDecideMsgToSend();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
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

  public void processNewComponent(Message m) throws InterruptedException {
    // change Parent, componentId and level
    myParent = m.getSenderId();
    componentId = m.getComponentId();
    level = m.getLevel();

  }

}