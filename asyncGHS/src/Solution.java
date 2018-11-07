
import java.util.List;

import java.util.Arrays;
import java.util.ArrayList;

public class Solution {

  public static void main(String args[]) {
    int[] a = new int[] { 1, 0, -1, 0, -2, 2 };
    fourSum(a, 0);
  }

  public static List<List<Integer>> fourSum(int[] nums, int target) {

    List<List<Integer>> l = new ArrayList<>();
    if (nums.length < 3)
      return l;

    Arrays.sort(nums);

    for (int i = 0; i < nums.length - 3; i++) {
      if (i != 0 && nums[i] == nums[i - 1])
        continue;

      for (int j = i + 1; j < nums.length - 2; j++) {
        if (j != 0 && nums[j] == nums[j - 1])
          continue;

        int lo = j + 1;
        int hi = nums.length - 1;

        while (lo < hi) {

          if (nums[i] + nums[j] + nums[lo] + nums[hi] > target) {
            hi--;
          } else if (nums[i] + nums[j] + nums[lo] + nums[hi] < target) {
            lo++;
          } else {

            List<Integer> l1 = new ArrayList<Integer>();
            l1.add(nums[i]);
            l1.add(nums[j]);
            l1.add(nums[lo]);
            l1.add(nums[hi]);
            l.add(l1);

            while (lo < hi && nums[lo] == nums[lo + 1])
              lo++;
            while (lo < hi && nums[hi] == nums[hi - 1])
              hi--;

            lo++;
            hi--;
          }
        }

      }
    }

    return l;
  }
}
