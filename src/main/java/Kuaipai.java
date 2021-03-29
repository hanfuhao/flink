import java.util.Arrays;

public class Kuaipai {
    public static void main(String[] args) {
        int[] a=new int[]{1,9,6,10,4,5,2,7};

             getSort(a,0,a.length-1);
        System.out.println(Arrays.toString(a));
    }
    public static void getSort(int[] a,int strat,int end){
        if(strat>end){
            return;
        }
            int i=strat;
            int j=end;
            int key=a[strat];

            while (i<j){
                while (i<j&&a[j]>key){
                    j--;
                }
                while (i<j && a[i]<=key){
                    i++;
                }
                if(i<j){
                    int tmp=0;
                    tmp=a[j];
                    a[j]=a[i];
                    a[i]=tmp;
                }
            }
            int p=a[i];
            a[i]=a[strat];
            a[strat]=p;
            System.out.println(i);
            getSort(a,strat,i-1);
            getSort(a,i+1,end);

    }
}
