import java.util.Arrays;

public class Charu {
    public static void main(String[] args) {
        int[] a=new int[] {1,9,7,5,3,2,4,15};
        getSort(a);
        System.out.println(Arrays.toString(a));
    }
    public static void getSort(int[] arr){
    for(int i=0;i<arr.length-1;i++){
     for(int j=i;j<arr.length-1;j++){
         if(arr[j+1]<arr[i]){
             int tmp=arr[j+1];
             arr[j+1]=arr[i];
             arr[i]=tmp;
         }
     }
    }
    }
}
