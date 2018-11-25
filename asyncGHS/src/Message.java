
import java.util.LinkedList;

public class Message {

  protected String mType;
  protected int senderId; // serial number in the graph
  protected int componentId;
  protected int round;
  protected double mwoe;
  protected LinkedList<Integer> path;
  protected int receiverId;
  protected int level;

  /**
   * Constructor for floodmax messages.
   * 
   * @param senderId
   * @param receiverId
   *          TODO
   * @param mwoe
   *          TODO
   * @param level
   *          TODO
   * @param round
   * @param componentId
   * @param mType
   */
  public Message(int senderId, int receiverId, double mwoe, int level, int round, int componentId, String mType) {
    this.senderId = senderId;
    this.receiverId = receiverId;
    this.mwoe = mwoe;
    this.componentId = componentId;
    this.level = level;
    this.round = round;
    this.mType = mType;
    this.path = new LinkedList<>();
  }

  public int getComponentId() {
    return componentId;
  }

  public void setComponentId(int componentId) {
    this.componentId = componentId;
  }

  public int getLevel() {
    return level;
  }

  public void setLevel(int level) {
    this.level = level;
  }

  public void setPath(LinkedList<Integer> path) {
    this.path = path;
  }

  protected boolean isLeader;

  public boolean isLeader() {
    return isLeader;
  }

  public void setLeader(boolean isLeader) {
    this.isLeader = isLeader;
  }

  public LinkedList<Integer> getPath() {
    return path;
  }

  public double getMwoe() {
    return mwoe;
  }

  public void setMwoe(double mwoe) {
    this.mwoe = mwoe;
  }

  public String toString() {
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("mType ").append(mType).append(" Sender id: ").append(senderId).append(" maxUid: ")
        .append(componentId).append(" round ").append(round);
    return stringBuffer.toString();
  }


  public int getReceiverId() {
    return receiverId;
  }

  public void setReceiverId(int receiverId) {
    this.receiverId = receiverId;
  }

  public int getSenderId() {
    return senderId;
  }

  public void setSenderId(int senderId) {
    this.senderId = senderId;
  }

  public int getUid() {
    return senderId;
  }

  public void setUid(int uid) {
    this.senderId = uid;
  }

  public int getRound() {
    return round;
  }

  public void setRound(int round) {
    this.round = round;
  }

  public String getmType() {
    return mType;
  }

  public void setmType(String mType) {
    this.mType = mType;
  }

}
