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

  public FloodMaxImplementation(int size, int[] ids, int[][] matrix) {
    this.size = size;
    this.ids = ids;
    this.matrix = matrix;
  }

  @Override
  public void run() {

    // Master thread create slave threads and run them
    MasterThread masterNode = new MasterThread(this.size, this.ids, this.matrix);
    masterNode.run();

    // Output the leaderId
    System.out.println("Leader is: " + masterNode.getLeaderId());

  }

}
