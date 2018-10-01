package message;

public class Message {

  public enum MessageType {
    ACK, REJECT, IAMLEADER, EXPLORE;
  }

  MessageType mType;
  int uid;
  int max_uid;
  String status;
  int round;

  public Message() {
    // TODO Auto-generated constructor stub
  }
  Message(int uid, int max_uid, String status) {
    this.uid = uid;
    this.max_uid = max_uid;
    this.status = status;
  }

  public int getUid() {
    return uid;
  }

  public void setUid(int uid) {
    this.uid = uid;
  }

  public int getMax_uid() {
    return max_uid;
  }

  public void setMax_uid(int max_uid) {
    this.max_uid = max_uid;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public int getRound() {
    return round;
  }

  public void setRound(int round) {
    this.round = round;
  }

  int get_uid() {
    return uid;
  }

  int get_max_uid() {
    return max_uid;
  }

  String get_status() {
    return status;
  }

  public MessageType getmType() {
    return mType;
  }

  public void setmType(MessageType mType) {
    this.mType = mType;
  }

}
