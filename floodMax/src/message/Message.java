package message;

public class Message {

  protected int from;
  protected int to;

  protected String mType;
  protected int senderId; // serial number in the graph
  protected int messageUid;
  protected int round;
  protected int distanceFromTo;

  public String toString() {
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("Sender id: ").append(senderId).append(" messageUid: ").append(messageUid).append(" round ")
        .append(round);
    return stringBuffer.toString();
  }

  /**
   * Constructor for floodmax messages.
   * 
   * @param senderId
   * @param maxUid
   * @param mType
   * @param round
   */
  public Message(int senderId, int round, int maxUid, String mType) {
    this.senderId = senderId;
    this.messageUid = maxUid;
    this.round = round;
    this.mType = mType;
    // this.newInfo = newInfo;
  }

  public int getSenderId() {
    return senderId;
  }

  public void setSenderId(int senderId) {
    this.senderId = senderId;
  }

  public int getMaxUid() {
    return messageUid;
  }

  public void setMaxUid(int messageUid) {
    this.messageUid = messageUid;
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

  public int getTo() {
    return to;
  }

  public void setTo(int to) {
    this.to = to;
  }

  public int getTarget() {
    return to;
  }

  public void setTarget(int target) {
    this.to = target;
  }

  public int getFrom() {
    return from;
  }

  public void setFrom(int initiator) {
    this.from = initiator;
  }

  public int getDistanceFromTo() {
    return distanceFromTo;
  }

  public void setDistanceFromTo(int distanceFromTo) {
    this.distanceFromTo = distanceFromTo;
  }
}
