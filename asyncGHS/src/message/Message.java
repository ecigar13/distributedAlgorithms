package message;

import java.util.LinkedList;

public class Message {

  protected String mType;
  protected int senderId; // serial number in the graph
  protected int maxUid;
  protected int round;
  protected double mwoe;
  protected LinkedList<Integer> path;

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
        .append(maxUid).append(" round ").append(round);
    return stringBuffer.toString();
  }

  /**
   * Constructor for floodmax messages.
   * 
   * @param senderId
   * @param mwoe
   *          TODO
   * @param round
   * @param maxUid
   * @param mType
   */
  public Message(int senderId, double mwoe, int round, int maxUid, String mType) {
    this.senderId = senderId;
    this.mwoe = mwoe;
    this.maxUid = maxUid;
    this.round = round;
    this.mType = mType;
    this.path = new LinkedList<>();
  }

  public int getSenderId() {
    return senderId;
  }

  public void setSenderId(int senderId) {
    this.senderId = senderId;
  }

  public int getMaxUid() {
    return maxUid;
  }

  public void setMaxUid(int messageUid) {
    this.maxUid = messageUid;
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
