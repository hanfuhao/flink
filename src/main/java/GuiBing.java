import java.util.Arrays;

public class GuiBing {

public static void main(String[] args) {
    int[] array = new int[10];
    for(int i = 0;i<array.length;i++){
        array[i] = (int)(Math.random()*100);
    }
    System.out.println(Arrays.toString(array));
    mergeSort(array, 0, array.length - 1);
    System.out.println(Arrays.toString(array));
}

    public static void mergeSort(int[] array, int left, int right) {
        if (left < right) {
            int center = (left + right) / 2;
            // 将数组拆分为两份，并递归拆分子数组，直到数组中只有一个元素
            mergeSort(array, left, center);
            mergeSort(array, center + 1, right);
            // 合并相邻数组
            merge(array, left, center, right);

        }
    }

    // 合并子数组的函数
    public static void merge(int[] array, int left, int center, int right) {
        // 临时数组，用于排序
        int[] tempArray = new int[array.length];
        // 用于将排好序的临时数组复制回原数组
        int mark = left;
        // 第二个数组的左端
        int mid = center + 1;
        // 用于临时数组的下标
        int tempLeft = left;
        while (left <= center && mid <= right) {
            // 从两个子数组中取出最小的放入临时数组，即按从小到大的顺序重新排布
            if (array[left] <= array[mid]) {
                tempArray[tempLeft++] = array[left++];
            } else {
                tempArray[tempLeft++] = array[mid++];
            }
        }
        // 剩余部分依次放入临时数组
        while (left <= center) {
            tempArray[tempLeft++] = array[left++];
        }
        while (mid <= right) {
            tempArray[tempLeft++] = array[mid++];
        }
        // 将中间数组中的内容复制回原数组
        while (mark <= right) {
            array[mark] = tempArray[mark++];
        }
    }

}