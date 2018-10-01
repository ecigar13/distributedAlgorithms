package floodMax;

public class FloodMaxImplementation implements Runnable {
  private int size;
  private int[] ids;
  private int[][] matrix;
  private int leader;

  public int getLeader() {
    return leader;
  }

  public void setLeader(int leader) {
    this.leader = leader;
  }

  public FloodMaxImplementation(int size, int[] ids2, int[][] matrix) {
    this.size = size;
    this.ids = ids2;
    this.matrix = matrix;
  }

  @Override
  public void run() {
    
    // TODO Auto-generated method stub
    
    //create master
    //create slaves
  }

}
