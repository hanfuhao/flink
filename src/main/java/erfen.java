import java.util.Arrays;

public class erfen {

    public static void main(String[] args) {
        int[]a=new int[]{1,2,3,4,5,6,12,18};

      int c=  getSort(a,12);
      System.out.println(c);

    }
    public static int getSort(int[] arr,int key){
        int start=0;
        int end=arr.length-1;
        int mid=0;
        while (start<=end){
             mid=(start+end)/2;
            if(arr[mid]<key){
                start=mid+1;
            }else if(arr[mid]>key){
                end=mid-1;
            }else{
                return mid;
            }

        }
        return -1;
    }

}
