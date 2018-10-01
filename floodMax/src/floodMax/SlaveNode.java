package floodMax;

import com.sun.org.apache.bcel.internal.generic.RETURN;

public class SlaveNode implements Runnable {

  private int id;
  private boolean finished;
  private boolean suspended;

  public enum Start {
    YES, NO;
  }

  private Start status = Start.YES;

  public SlaveNode() {
    // TODO Auto-generated constructor stub
  }

  void suspend() {
    suspended = true;
  }

  public boolean resume() {
    if (status.equals(Start.YES)) {
      suspended = false;
      notify();
      return true;
    } else
      return false;
  }

  public Start getStatus() {
    return status;
  }

  public void setStatus(Start status) {
    this.status = status;
  }

  public boolean isFinished() {
    return finished;
  }

  public void setFinished(boolean finished) {
    this.finished = finished;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  @Override
  public void run() {
    // TODO Auto-generated method stub

  }

}
